package domain

import io.circe.Json
import openservicebroker.model.LastOperationResourceEnums
import openservicebroker.model.ServiceEnums

/**
  * `CanEqual` instances for types compared across services.
  *
  * The codegen output cannot be edited, so equality with `-language:strictEquality` has to be
  * enabled from this side via given `CanEqual` instances.
  */

given CanEqual[LastOperationResourceEnums.State, LastOperationResourceEnums.State] =
  CanEqual.derived

given CanEqual[ServiceEnums.Requires, ServiceEnums.Requires] = CanEqual.derived
given CanEqual[Json, Json]                                   = CanEqual.derived
given CanEqual[Option[Json], Option[Json]]                   = CanEqual.derived
