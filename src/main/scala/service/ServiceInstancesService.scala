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

object ServiceInstancesService {

  enum OperationKind derives CanEqual {
    case Provision, Update, Deprovision
  }

  object OperationKind {

    def asString(k: OperationKind): String = k match {
      case Provision   => "provision"
      case Update      => "update"
      case Deprovision => "deprovision"
    }

    def fromString(s: String): OperationKind = s match {
      case "provision"   => Provision
      case "update"      => Update
      case "deprovision" => Deprovision
      case other         => throw new RuntimeException(s"Unknown operation_kind: $other")
    }

  }

  final case class Operation(
      id: String,
      kind: OperationKind,
      completeAt: Instant,
      state: OpState = OpState.`in progress`,
      description: Option[String] = None
  )

  final case class InstanceState(
      serviceId: String,
      planId: String,
      parameters: Option[Json],
      context: Option[Json],
      maintenanceInfo: Option[MaintenanceInfo],
      dashboardUrl: Option[String],
      operation: Option[Operation],
      deleted: Boolean
  )

  given CanEqual[InstanceState, InstanceState] = CanEqual.derived

  enum ProvisionResult {

    case AlreadyExists(response: ServiceInstanceProvisionResponse)
    case Created(response: ServiceInstanceProvisionResponse)
    case Accepted(op: ServiceInstanceAsyncOperation)

  }

  enum UpdateResult {

    case Updated
    case Accepted(operation: Option[String])

  }

  enum DeprovisionResult {

    case Deleted
    case Accepted(operation: Option[String])

  }

  /**
    * How long a simulated async operation takes to "complete".
    */
  val AsyncDelay: FiniteDuration = 2.seconds

  type SessionPool = Resource[IO, Session[IO]]

  def postgres(catalog: CatalogService, pool: SessionPool): ServiceInstancesService =
    new ServiceInstancesServiceLive(catalog, pool)

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

  private final case class InstanceRow(
      serviceId: String,
      planId: String,
      parameters: Option[Json],
      context: Option[Json],
      maintenanceInfo: Option[Json],
      dashboardUrl: Option[String],
      operationId: Option[String],
      operationKind: Option[OperationKind],
      operationState: Option[OpState],
      operationCompleteAt: Option[Instant],
      deleted: Boolean
  )

  private val instanceRowCodec: Codec[InstanceRow] =
    (text *: text *: jsonbCodec.opt *: jsonbCodec.opt *: jsonbCodec.opt *:
      text.opt *: text.opt *: opKindCodec.opt *: opStateCodec.opt *: instantCodec.opt *: bool)
      .to[InstanceRow]

  private def jsonToMaintenanceInfo(j: Json): MaintenanceInfo = MaintenanceInfo(
    version = j.hcursor.get[String]("version").getOrElse(""),
    description = j.hcursor.get[String]("description").toOption
  )

  private def maintenanceInfoToJson(m: MaintenanceInfo): Json = Json.obj(
    "version"     -> Json.fromString(m.version),
    "description" -> m.description.fold(Json.Null)(Json.fromString)
  )

  private def rowToState(r: InstanceRow): InstanceState = {
    val op: Option[Operation] =
      (r.operationId, r.operationKind, r.operationCompleteAt, r.operationState) match {
        case (Some(id), Some(kind), Some(at), Some(st)) => Some(Operation(id, kind, at, st))
        case _                                          => None
      }
    InstanceState(
      serviceId = r.serviceId,
      planId = r.planId,
      parameters = r.parameters,
      context = r.context,
      maintenanceInfo = r.maintenanceInfo.map(jsonToMaintenanceInfo),
      dashboardUrl = r.dashboardUrl,
      operation = op,
      deleted = r.deleted
    )
  }

  private def stateToRow(s: InstanceState): InstanceRow = InstanceRow(
    serviceId = s.serviceId,
    planId = s.planId,
    parameters = s.parameters,
    context = s.context,
    maintenanceInfo = s.maintenanceInfo.map(maintenanceInfoToJson),
    dashboardUrl = s.dashboardUrl,
    operationId = s.operation.map(_.id),
    operationKind = s.operation.map(_.kind),
    operationState = s.operation.map(_.state),
    operationCompleteAt = s.operation.map(_.completeAt),
    deleted = s.deleted
  )

  // ---------- Queries / Commands ----------

  private val selectByInstanceId: Query[String, InstanceRow] =
    sql"""
      SELECT service_id, plan_id, parameters, context, maintenance_info,
             dashboard_url, operation_id, operation_kind, operation_state,
             operation_complete_at, deleted
      FROM service_instances
      WHERE instance_id = $text
    """.query(instanceRowCodec)

  private val insertInstance: Command[(String, InstanceRow)] =
    sql"""
      INSERT INTO service_instances (
        instance_id, service_id, plan_id, parameters, context, maintenance_info,
        dashboard_url, operation_id, operation_kind, operation_state,
        operation_complete_at, deleted
      ) VALUES ($text, ${instanceRowCodec.values})
    """.command

  private val updateInstance: Command[(InstanceRow, String)] =
    sql"""
      UPDATE service_instances SET
        service_id            = $text,
        plan_id               = $text,
        parameters            = ${jsonbCodec.opt},
        context               = ${jsonbCodec.opt},
        maintenance_info      = ${jsonbCodec.opt},
        dashboard_url         = ${text.opt},
        operation_id          = ${text.opt},
        operation_kind        = ${opKindCodec.opt},
        operation_state       = ${opStateCodec.opt},
        operation_complete_at = ${instantCodec.opt},
        deleted               = $bool,
        updated_at            = NOW()
      WHERE instance_id = $text
    """.command.contramap[(InstanceRow, String)] { case (r, id) =>
      (
        r.serviceId,
        r.planId,
        r.parameters,
        r.context,
        r.maintenanceInfo,
        r.dashboardUrl,
        r.operationId,
        r.operationKind,
        r.operationState,
        r.operationCompleteAt,
        r.deleted,
        id
      )
    }

  private val deleteInstance: Command[String] =
    sql"DELETE FROM service_instances WHERE instance_id = $text".command

  // ---------- Implementation ----------

  private final class ServiceInstancesServiceLive(catalog: CatalogService, pool: SessionPool)
      extends ServiceInstancesService {

    private def now: IO[Instant] = IO.realTime.map(d => Instant.ofEpochMilli(d.toMillis))

    private def newOperation(kind: OperationKind): IO[Operation] =
      now.map(t => Operation(UUID.randomUUID().toString, kind, t.plusMillis(AsyncDelay.toMillis)))

    private def needsAsync(plan: Plan): Boolean =
      plan.maximumPollingDuration.isDefined

    private def settle(state: InstanceState, t: Instant): InstanceState = state.operation match {
      case Some(op) if !t.isBefore(op.completeAt) && (op.state == OpState.`in progress`) =>
        op.kind match {
          case OperationKind.Deprovision =>
            state.copy(operation = Some(op.copy(state = OpState.succeeded)), deleted = true)
          case _ =>
            state.copy(operation = Some(op.copy(state = OpState.succeeded)))
        }
      case _ => state
    }

    private def loadInstance(session: Session[IO], instanceId: String): IO[Option[InstanceState]] =
      session.prepare(selectByInstanceId).flatMap { ps =>
        ps.option(instanceId).map(_.map(rowToState))
      }

    def provision(
        instanceId: String,
        body: ServiceInstanceProvisionRequestBody,
        acceptsIncomplete: Boolean
    ): IO[Either[BrokerError, ProvisionResult]] = {
      catalog.findPlan(body.serviceId, body.planId).flatMap {
        case None =>
          IO.pure(Left(BrokerError.UnknownPlan))
        case Some(plan) =>
          val mustAsync = needsAsync(plan)
          if (mustAsync && !acceptsIncomplete) {
            IO.pure(Left(BrokerError.AsyncRequired))
          } else {
            pool.use { session =>
              session.transaction.use { _ =>
                loadInstance(session, instanceId).flatMap {
                  case Some(existing) if !existing.deleted =>
                    val same = (existing.serviceId == body.serviceId) &&
                      (existing.planId == body.planId) &&
                      (existing.parameters == body.parameters) &&
                      (existing.context == body.context)
                    if (same) {
                      val res =
                        ServiceInstanceProvisionResponse(dashboardUrl = existing.dashboardUrl)
                      IO.pure(
                        Right(ProvisionResult.AlreadyExists(res)): Either[
                          BrokerError,
                          ProvisionResult
                        ]
                      )
                    } else {
                      IO.pure(
                        Left(
                          BrokerError
                            .Conflict("Service instance already exists with different attributes")
                        )
                      )
                    }
                  case _ =>
                    newOperation(OperationKind.Provision).flatMap { op =>
                      val async = mustAsync && acceptsIncomplete
                      val state = InstanceState(
                        serviceId = body.serviceId,
                        planId = body.planId,
                        parameters = body.parameters,
                        context = body.context,
                        maintenanceInfo = None,
                        dashboardUrl = None,
                        operation = if (async) Some(op) else None,
                        deleted = false
                      )
                      session.prepare(insertInstance).flatMap { ps =>
                        ps.execute((instanceId, stateToRow(state))).as {
                          if (async) {
                            Right(
                              ProvisionResult.Accepted(
                                ServiceInstanceAsyncOperation(operation = Some(op.id))
                              )
                            ): Either[BrokerError, ProvisionResult]
                          } else {
                            Right(ProvisionResult.Created(ServiceInstanceProvisionResponse()))
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

    def update(
        instanceId: String,
        body: ServiceInstanceUpdateRequestBody,
        acceptsIncomplete: Boolean
    ): IO[Either[BrokerError, UpdateResult]] = {
      pool.use { session =>
        session.transaction.use { _ =>
          loadInstance(session, instanceId).flatMap {
            case None =>
              IO.pure(Left(BrokerError.NotFound): Either[BrokerError, UpdateResult])
            case Some(existing) if existing.deleted =>
              IO.pure(Left(BrokerError.NotFound))
            case Some(existing) if existing.serviceId != body.serviceId =>
              IO.pure(Left(BrokerError.ServiceIdMismatch))
            case Some(existing) =>
              val newPlanId = body.planId.getOrElse(existing.planId)
              catalog.findPlan(existing.serviceId, newPlanId).flatMap {
                case None =>
                  IO.pure(Left(BrokerError.UnknownPlan))
                case Some(plan) =>
                  val mustAsync = needsAsync(plan)
                  if (mustAsync && !acceptsIncomplete) {
                    IO.pure(Left(BrokerError.AsyncRequired))
                  } else {
                    newOperation(OperationKind.Update).flatMap { op =>
                      val async   = mustAsync && acceptsIncomplete
                      val updated = existing.copy(
                        planId = newPlanId,
                        parameters = body.parameters.orElse(existing.parameters),
                        context = body.context.orElse(existing.context),
                        operation = if (async) Some(op) else None
                      )
                      session.prepare(updateInstance).flatMap { ps =>
                        ps.execute((stateToRow(updated), instanceId)).as {
                          if (async) Right(UpdateResult.Accepted(Some(op.id)))
                          else Right(UpdateResult.Updated)
                        }
                      }
                    }
                  }
              }
          }
        }
      }
    }

    def deprovision(
        instanceId: String,
        serviceId: String,
        planId: String,
        acceptsIncomplete: Boolean
    ): IO[Either[BrokerError, DeprovisionResult]] = {
      catalog.findPlan(serviceId, planId).flatMap { planOpt =>
        val mustAsync = planOpt.exists(needsAsync)
        if (mustAsync && !acceptsIncomplete) {
          IO.pure(Left(BrokerError.AsyncRequired))
        } else {
          pool.use { session =>
            session.transaction.use { _ =>
              now.flatMap { t =>
                newOperation(OperationKind.Deprovision).flatMap { op =>
                  loadInstance(session, instanceId).flatMap {
                    case None =>
                      IO.pure(Left(BrokerError.Gone): Either[BrokerError, DeprovisionResult])
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
                          session.prepare(updateInstance).flatMap { ps =>
                            ps.execute((stateToRow(withOp), instanceId))
                              .as(
                                Right(DeprovisionResult.Accepted(Some(op.id)))
                              )
                          }
                        } else {
                          session.prepare(deleteInstance).flatMap { ps =>
                            ps.execute(instanceId).as(Right(DeprovisionResult.Deleted))
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
        serviceId: Option[String],
        planId: Option[String]
    ): IO[Either[BrokerError, ServiceInstanceResource]] = {
      pool.use { session =>
        session.transaction.use { _ =>
          now.flatMap { t =>
            loadInstance(session, instanceId).flatMap {
              case None =>
                IO.pure(Left(BrokerError.NotFound): Either[BrokerError, ServiceInstanceResource])
              case Some(raw) =>
                val s                 = settle(raw, t)
                val persist: IO[Unit] =
                  if (s == raw) IO.unit
                  else if (s.deleted) {
                    session.prepare(deleteInstance).flatMap(_.execute(instanceId)).void
                  } else {
                    session
                      .prepare(updateInstance)
                      .flatMap(_.execute((stateToRow(s), instanceId)))
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
                      ServiceInstanceResource(
                        serviceId = Some(s.serviceId),
                        planId = Some(s.planId),
                        dashboardUrl = s.dashboardUrl,
                        parameters = s.parameters,
                        maintenanceInfo = s.maintenanceInfo
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
        serviceId: Option[String],
        planId: Option[String],
        operationId: Option[String]
    ): IO[Either[BrokerError, LastOperationResource]] = {
      pool.use { session =>
        session.transaction.use { _ =>
          now.flatMap { t =>
            loadInstance(session, instanceId).flatMap {
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
                        session.prepare(deleteInstance).flatMap(_.execute(instanceId)).void
                      } else {
                        session
                          .prepare(updateInstance)
                          .flatMap(_.execute((stateToRow(s), instanceId)))
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

trait ServiceInstancesService {

  import ServiceInstancesService.*

  def provision(
      instanceId: String,
      body: ServiceInstanceProvisionRequestBody,
      acceptsIncomplete: Boolean
  ): IO[Either[BrokerError, ProvisionResult]]

  def update(
      instanceId: String,
      body: ServiceInstanceUpdateRequestBody,
      acceptsIncomplete: Boolean
  ): IO[Either[BrokerError, UpdateResult]]

  def deprovision(
      instanceId: String,
      serviceId: String,
      planId: String,
      acceptsIncomplete: Boolean
  ): IO[Either[BrokerError, DeprovisionResult]]

  def fetch(
      instanceId: String,
      serviceId: Option[String],
      planId: Option[String]
  ): IO[Either[BrokerError, ServiceInstanceResource]]

  def lastOperation(
      instanceId: String,
      serviceId: Option[String],
      planId: Option[String],
      operationId: Option[String]
  ): IO[Either[BrokerError, LastOperationResource]]

}
