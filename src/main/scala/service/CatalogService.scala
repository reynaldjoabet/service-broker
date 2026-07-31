package service

import cats.effect.IO

import openservicebroker.model.{Catalog, Plan, Service}

trait CatalogService {

  def get: IO[Catalog]
  def findService(serviceId: String): IO[Option[Service]]
  def findPlan(serviceId: String, planId: String): IO[Option[Plan]]

}

object CatalogService {

  def static(catalog: Catalog): CatalogService = new CatalogService {
    private val services: Seq[Service]            = catalog.services.getOrElse(Seq.empty)
    private val byServiceId: Map[String, Service] = services.map(s => s.id -> s).toMap

    def get: IO[Catalog] = IO.pure(catalog)

    def findService(serviceId: String): IO[Option[Service]] =
      IO.pure(byServiceId.get(serviceId))

    def findPlan(serviceId: String, planId: String): IO[Option[Plan]] =
      IO.pure(byServiceId.get(serviceId).flatMap(_.plans.find(_.id == planId)))
  }

  val default: CatalogService = static(
    Catalog(services =
      Some(
        Seq(
          Service(
            name = "example-service",
            id = "5a8d8bba-e3d3-4d8a-9d2a-5e8c2a3a1b00",
            description = "An example managed service exposed by this broker",
            bindable = true,
            tags = Some(Seq("example", "demo")),
            planUpdateable = Some(true),
            plans =
              Seq(
                Plan(
                  id = "f1d6f3a4-0c19-4f1f-b3e3-2c4d2f7c8a01",
                  name = "small",
                  description = "Single-tenant, low-throughput plan",
                  free = Some(true),
                  bindable = Some(true)
                ),
                Plan(
                  id = "a92d2a3a-8bf2-4e7b-9b5d-2a3d4e5f6a02",
                  name = "large",
                  description = "Dedicated, high-throughput plan",
                  free = Some(false),
                  bindable = Some(true)
                )
              )
          )
        )
      )
    )
  )

}
