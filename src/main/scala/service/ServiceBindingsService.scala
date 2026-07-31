package service

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*

import domain.given
import domain.BrokerError
import io.circe.{parser as JsonParser, Json}
import openservicebroker.model.*
import openservicebroker.model.LastOperationResourceEnums.State as OpState
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

object ServiceBindingsService {

  enum OperationKind derives CanEqual {
    case Bind, Unbind
  }

  object OperationKind {

    def asString(k: OperationKind): String = k match {
      case Bind   => "bind"
      case Unbind => "unbind"
    }

    def fromString(s: String): OperationKind = s match {
      case "bind"   => Bind
      case "unbind" => Unbind
      case other    => throw new RuntimeException(s"Unknown operation_kind: $other")
    }

  }

  final case class Operation(
      id: String,
      kind: OperationKind,
      completeAt: Instant,
      state: OpState = OpState.`in progress`,
      description: Option[String] = None
  )

  final case class BindingState(
      instanceId: String,
      serviceId: String,
      planId: String,
      parameters: Option[Json],
      context: Option[Json],
      credentials: Option[Json],
      syslogDrainUrl: Option[String],
      routeServiceUrl: Option[String],
      operation: Option[Operation],
      deleted: Boolean
  )

  given CanEqual[BindingState, BindingState] = CanEqual.derived

  enum BindResult {

    case AlreadyExists(response: ServiceBindingResponse)
    case Created(response: ServiceBindingResponse)
    case Accepted(operation: Option[String])

  }

  enum UnbindResult {

    case Deleted
    case Accepted(operation: Option[String])

  }

  val AsyncDelay: FiniteDuration = 2.seconds

  type SessionPool = Resource[IO, Session[IO]]

  def postgres(
      catalog: CatalogService,
      instances: ServiceInstancesService,
      pool: SessionPool
  ): ServiceBindingsService = new ServiceBindingsServiceLive(catalog, instances, pool)

  // ---------- Codecs ----------

  private val jsonbCodec: Codec[Json] = text.imap[Json] { s =>
    JsonParser.parse(s).getOrElse(Json.Null)
  }(_.noSpaces)

  private val opStateCodec: Codec[OpState] = text.imap[OpState] {
    case "in progress" => OpState.`in progress`
    case "succeeded"   => OpState.succeeded
    case "failed"      => OpState.failed
    case other         => throw new RuntimeException(s"Unknown operation_state: $other")
  } {
    case OpState.`in progress` => "in progress"
    case OpState.succeeded     => "succeeded"
    case OpState.failed        => "failed"
  }

  private val opKindCodec: Codec[OperationKind] =
    text.imap[OperationKind](OperationKind.fromString)(OperationKind.asString)

  private val instantCodec: Codec[Instant] =
    timestamptz.imap(_.toInstant)(i => OffsetDateTime.ofInstant(i, ZoneOffset.UTC))

  private final case class BindingRow(
      serviceId: String,
      planId: String,
      parameters: Option[Json],
      context: Option[Json],
      credentials: Option[Json],
      syslogDrainUrl: Option[String],
      routeServiceUrl: Option[String],
      operationId: Option[String],
      operationKind: Option[OperationKind],
      operationState: Option[OpState],
      operationCompleteAt: Option[Instant],
      deleted: Boolean
  )

  private val bindingRowCodec: Codec[BindingRow] =
    (text *: text *: jsonbCodec.opt *: jsonbCodec.opt *: jsonbCodec.opt *:
      text.opt *: text.opt *:
      text.opt *: opKindCodec.opt *: opStateCodec.opt *: instantCodec.opt *: bool)
      .to[BindingRow]

  private def rowToState(instanceId: String, r: BindingRow): BindingState = {
    val op: Option[Operation] =
      (r.operationId, r.operationKind, r.operationCompleteAt, r.operationState) match {
        case (Some(id), Some(kind), Some(at), Some(st)) => Some(Operation(id, kind, at, st))
        case _                                          => None
      }
    BindingState(
      instanceId = instanceId,
      serviceId = r.serviceId,
      planId = r.planId,
      parameters = r.parameters,
      context = r.context,
      credentials = r.credentials,
      syslogDrainUrl = r.syslogDrainUrl,
      routeServiceUrl = r.routeServiceUrl,
      operation = op,
      deleted = r.deleted
    )
  }

  private def stateToRow(s: BindingState): BindingRow = BindingRow(
    serviceId = s.serviceId,
    planId = s.planId,
    parameters = s.parameters,
    context = s.context,
    credentials = s.credentials,
    syslogDrainUrl = s.syslogDrainUrl,
    routeServiceUrl = s.routeServiceUrl,
    operationId = s.operation.map(_.id),
    operationKind = s.operation.map(_.kind),
    operationState = s.operation.map(_.state),
    operationCompleteAt = s.operation.map(_.completeAt),
    deleted = s.deleted
  )

  // ---------- Queries / Commands ----------

  private val selectByKey: Query[(String, String), BindingRow] =
    sql"""
      SELECT service_id, plan_id, parameters, context, credentials,
             syslog_drain_url, route_service_url,
             operation_id, operation_kind, operation_state,
             operation_complete_at, deleted
      FROM service_bindings
      WHERE instance_id = $text AND binding_id = $text
    """.query(bindingRowCodec)

  private val insertBinding: Command[(String, String, BindingRow)] =
    sql"""
      INSERT INTO service_bindings (
        instance_id, binding_id, service_id, plan_id, parameters, context, credentials,
        syslog_drain_url, route_service_url,
        operation_id, operation_kind, operation_state, operation_complete_at, deleted
      ) VALUES ($text, $text, ${bindingRowCodec.values})
    """.command

  private val updateBinding: Command[(BindingRow, String, String)] =
    sql"""
      UPDATE service_bindings SET
        service_id            = $text,
        plan_id               = $text,
        parameters            = ${jsonbCodec.opt},
        context               = ${jsonbCodec.opt},
        credentials           = ${jsonbCodec.opt},
        syslog_drain_url      = ${text.opt},
        route_service_url     = ${text.opt},
        operation_id          = ${text.opt},
        operation_kind        = ${opKindCodec.opt},
        operation_state       = ${opStateCodec.opt},
        operation_complete_at = ${instantCodec.opt},
        deleted               = $bool,
        updated_at            = NOW()
      WHERE instance_id = $text AND binding_id = $text
    """.command.contramap[(BindingRow, String, String)] { case (r, iid, bid) =>
      (
        r.serviceId,
        r.planId,
        r.parameters,
        r.context,
        r.credentials,
        r.syslogDrainUrl,
        r.routeServiceUrl,
        r.operationId,
        r.operationKind,
        r.operationState,
        r.operationCompleteAt,
        r.deleted,
        iid,
        bid
      )
    }

  private val deleteBinding: Command[(String, String)] =
    sql"DELETE FROM service_bindings WHERE instance_id = $text AND binding_id = $text".command

  // ---------- Implementation ----------

  private final class ServiceBindingsServiceLive(
      catalog: CatalogService,
      instances: ServiceInstancesService,
      pool: SessionPool
  ) extends ServiceBindingsService {

    private def now: IO[Instant] = IO.realTime.map(d => Instant.ofEpochMilli(d.toMillis))

    private def newOperation(kind: OperationKind): IO[Operation] =
      now.map(t => Operation(UUID.randomUUID().toString, kind, t.plusMillis(AsyncDelay.toMillis)))

    private def needsAsync(plan: Plan): Boolean = plan.maximumPollingDuration.isDefined

    private def settle(state: BindingState, t: Instant): BindingState = state.operation match {
      case Some(op) if !t.isBefore(op.completeAt) && (op.state == OpState.`in progress`) =>
        op.kind match {
          case OperationKind.Unbind =>
            state.copy(operation = Some(op.copy(state = OpState.succeeded)), deleted = true)
          case OperationKind.Bind =>
            state.copy(operation = Some(op.copy(state = OpState.succeeded)))
        }
      case _ => state
    }

    private def fakeCredentials(instanceId: String, bindingId: String): Json =
      Json.obj(
        "uri"      -> Json.fromString(s"postgres://user:pwd@example.com/$instanceId"),
        "username" -> Json.fromString(s"user-$bindingId"),
        "password" -> Json.fromString("changeme")
      )

    private def loadBinding(
        session: Session[IO],
        instanceId: String,
        bindingId: String
    ): IO[Option[BindingState]] =
      session.prepare(selectByKey).flatMap { ps =>
        ps.option((instanceId, bindingId)).map(_.map(rowToState(instanceId, _)))
      }

    def bind(
        instanceId: String,
        bindingId: String,
        body: ServiceBindingRequest,
        acceptsIncomplete: Boolean
    ): IO[Either[BrokerError, BindResult]] = {
      instances.fetch(instanceId, Some(body.serviceId), Some(body.planId)).flatMap {
        case Left(BrokerError.NotFound) =>
          IO.pure(Left(BrokerError.Conflict("Instance does not exist")))
        case Left(BrokerError.ServiceIdMismatch) =>
          IO.pure(Left(BrokerError.ServiceIdMismatch))
        case Left(BrokerError.PlanIdMismatch) =>
          IO.pure(Left(BrokerError.PlanIdMismatch))
        case Left(other) =>
          IO.pure(Left(other))
        case Right(_) =>
          catalog.findPlan(body.serviceId, body.planId).flatMap {
            case None =>
              IO.pure(Left(BrokerError.UnknownPlan))
            case Some(plan) =>
              val mustAsync = needsAsync(plan)
              if (mustAsync && !acceptsIncomplete) {
                IO.pure(Left(BrokerError.AsyncRequired))
              } else {
                newOperation(OperationKind.Bind).flatMap { op =>
                  val async       = mustAsync && acceptsIncomplete
                  val credentials = fakeCredentials(instanceId, bindingId)
                  pool.use { session =>
                    session.transaction.use { _ =>
                      loadBinding(session, instanceId, bindingId).flatMap {
                        case Some(existing) if !existing.deleted =>
                          val same = (existing.serviceId == body.serviceId) &&
                            (existing.planId == body.planId) &&
                            (existing.parameters == body.parameters) &&
                            (existing.context == body.context)
                          if (same) {
                            val res = ServiceBindingResponse(credentials = existing.credentials)
                            IO.pure(
                              Right(BindResult.AlreadyExists(res)): Either[BrokerError, BindResult]
                            )
                          } else {
                            IO.pure(
                              Left(
                                BrokerError.Conflict(
                                  "Service binding already exists with different attributes"
                                )
                              )
                            )
                          }
                        case _ =>
                          val state = BindingState(
                            instanceId = instanceId,
                            serviceId = body.serviceId,
                            planId = body.planId,
                            parameters = body.parameters,
                            context = body.context,
                            credentials = if (async) None else Some(credentials),
                            syslogDrainUrl = None,
                            routeServiceUrl = None,
                            operation = if (async) Some(op) else None,
                            deleted = false
                          )
                          session.prepare(insertBinding).flatMap { ps =>
                            ps.execute((instanceId, bindingId, stateToRow(state))).as {
                              if (async) {
                                Right(BindResult.Accepted(Some(op.id))): Either[
                                  BrokerError,
                                  BindResult
                                ]
                              } else {
                                Right(
                                  BindResult.Created(
                                    ServiceBindingResponse(credentials = Some(credentials))
                                  )
                                )
                              }
                            }
                          }
                      }
                    }
                  }
                }
              }
          }
      }
    }

    def unbind(
        instanceId: String,
        bindingId: String,
        serviceId: String,
        planId: String,
        acceptsIncomplete: Boolean
    ): IO[Either[BrokerError, UnbindResult]] = {
      catalog.findPlan(serviceId, planId).flatMap { planOpt =>
        val mustAsync = planOpt.exists(needsAsync)
        if (mustAsync && !acceptsIncomplete) {
          IO.pure(Left(BrokerError.AsyncRequired))
        } else {
          pool.use { session =>
            session.transaction.use { _ =>
              now.flatMap { t =>
                newOperation(OperationKind.Unbind).flatMap { op =>
                  loadBinding(session, instanceId, bindingId).flatMap {
                    case None =>
                      IO.pure(Left(BrokerError.Gone): Either[BrokerError, UnbindResult])
                    case Some(existing) =>
                      val s = settle(existing, t)
                      if (s.deleted) {
                        IO.pure(Left(BrokerError.Gone))
                      } else if (s.serviceId != serviceId) {
                        IO.pure(Left(BrokerError.ServiceIdMismatch))
                      } else if (s.planId != planId) {
                        IO.pure(Left(BrokerError.PlanIdMismatch))
                      } else {
                        val async = mustAsync && acceptsIncomplete
                        if (async) {
                          val withOp = s.copy(operation = Some(op))
                          session.prepare(updateBinding).flatMap { ps =>
                            ps.execute((stateToRow(withOp), instanceId, bindingId))
                              .as(
                                Right(UnbindResult.Accepted(Some(op.id)))
                              )
                          }
                        } else {
                          session.prepare(deleteBinding).flatMap { ps =>
                            ps.execute((instanceId, bindingId)).as(Right(UnbindResult.Deleted))
                          }
                        }
                      }
                  }
                }
              }
            }
          }
        }
      }
    }

    def fetch(
        instanceId: String,
        bindingId: String,
        serviceId: Option[String],
        planId: Option[String]
    ): IO[Either[BrokerError, ServiceBindingResource]] = {
      pool.use { session =>
        session.transaction.use { _ =>
          now.flatMap { t =>
            loadBinding(session, instanceId, bindingId).flatMap {
              case None =>
                IO.pure(Left(BrokerError.NotFound): Either[BrokerError, ServiceBindingResource])
              case Some(raw) =>
                val s                 = settle(raw, t)
                val persist: IO[Unit] =
                  if (s == raw) IO.unit
                  else if (s.deleted) {
                    session.prepare(deleteBinding).flatMap(_.execute((instanceId, bindingId))).void
                  } else {
                    session
                      .prepare(updateBinding)
                      .flatMap(_.execute((stateToRow(s), instanceId, bindingId)))
                      .void
                  }
                persist.as {
                  if (s.deleted) {
                    Left(BrokerError.NotFound)
                  } else if (s.operation.exists(_.state == OpState.`in progress`)) {
                    Left(BrokerError.ConcurrencyConflict)
                  } else if (serviceId.exists(_ != s.serviceId)) {
                    Left(BrokerError.ServiceIdMismatch)
                  } else if (planId.exists(_ != s.planId)) {
                    Left(BrokerError.PlanIdMismatch)
                  } else {
                    Right(
                      ServiceBindingResource(
                        credentials = s.credentials,
                        syslogDrainUrl = s.syslogDrainUrl,
                        routeServiceUrl = s.routeServiceUrl,
                        parameters = s.parameters
                      )
                    )
                  }
                }
            }
          }
        }
      }
    }

    def lastOperation(
        instanceId: String,
        bindingId: String,
        serviceId: Option[String],
        planId: Option[String],
        operationId: Option[String]
    ): IO[Either[BrokerError, LastOperationResource]] = {
      pool.use { session =>
        session.transaction.use { _ =>
          now.flatMap { t =>
            loadBinding(session, instanceId, bindingId).flatMap {
              case None =>
                IO.pure(Left(BrokerError.NotFound): Either[BrokerError, LastOperationResource])
              case Some(raw) =>
                raw.operation match {
                  case None =>
                    IO.pure(Left(BrokerError.NotFound))
                  case Some(op) if operationId.exists(_ != op.id) =>
                    IO.pure(Left(BrokerError.NotFound))
                  case Some(_) =>
                    val s                 = settle(raw, t)
                    val persist: IO[Unit] =
                      if (s == raw) IO.unit
                      else if (s.deleted) {
                        session
                          .prepare(deleteBinding)
                          .flatMap(_.execute((instanceId, bindingId)))
                          .void
                      } else {
                        session
                          .prepare(updateBinding)
                          .flatMap(_.execute((stateToRow(s), instanceId, bindingId)))
                          .void
                      }
                    persist.as(
                      Right(
                        LastOperationResource(
                          state = s.operation.get.state,
                          description = s.operation.get.description
                        )
                      )
                    )
                }
            }
          }
        }
      }
    }

  }

}

trait ServiceBindingsService {

  import ServiceBindingsService.*

  def bind(
      instanceId: String,
      bindingId: String,
      body: ServiceBindingRequest,
      acceptsIncomplete: Boolean
  ): IO[Either[BrokerError, BindResult]]

  def unbind(
      instanceId: String,
      bindingId: String,
      serviceId: String,
      planId: String,
      acceptsIncomplete: Boolean
  ): IO[Either[BrokerError, UnbindResult]]

  def fetch(
      instanceId: String,
      bindingId: String,
      serviceId: Option[String],
      planId: Option[String]
  ): IO[Either[BrokerError, ServiceBindingResource]]

  def lastOperation(
      instanceId: String,
      bindingId: String,
      serviceId: Option[String],
      planId: Option[String],
      operationId: Option[String]
  ): IO[Either[BrokerError, LastOperationResource]]

}
