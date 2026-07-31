package domain

sealed abstract class BrokerError(val description: String) derives CanEqual

object BrokerError {

  case object NotFound                      extends BrokerError("Resource not found")
  case object Gone                          extends BrokerError("Resource has already been deleted")
  final case class Conflict(detail: String) extends BrokerError(detail)
  case object ConcurrencyConflict
      extends BrokerError("Another operation is in progress for this resource")
  case object AsyncRequired
      extends BrokerError(
        "This service plan requires client support for asynchronous service operations"
      )
  case object ServiceIdMismatch
      extends BrokerError(
        "The supplied service_id does not match the service_id of the existing resource"
      )
  case object PlanIdMismatch
      extends BrokerError(
        "The supplied plan_id does not match the plan_id of the existing resource"
      )
  case object PlanChangeNotSupported
      extends BrokerError("The requested plan cannot be updated from the current plan")
  case object MaintenanceInfoConflict
      extends BrokerError("The maintenance_info.version of the request does not match the catalog")
  case object UnknownService                  extends BrokerError("The requested service_id is not in the catalog")
  case object UnknownPlan                     extends BrokerError("The requested plan_id is not in the catalog")
  final case class Validation(detail: String) extends BrokerError(detail)

  extension (e: BrokerError) {

    def code: String = e match {
      case NotFound                => "NotFound"
      case Gone                    => "Gone"
      case _: Conflict             => "Conflict"
      case ConcurrencyConflict     => "ConcurrencyError"
      case AsyncRequired           => "AsyncRequired"
      case ServiceIdMismatch       => "Conflict"
      case PlanIdMismatch          => "Conflict"
      case PlanChangeNotSupported  => "PlanChangeNotSupported"
      case MaintenanceInfoConflict => "MaintenanceInfoConflict"
      case UnknownService          => "Conflict"
      case UnknownPlan             => "Conflict"
      case _: Validation           => "BadRequest"
    }

  }

}
