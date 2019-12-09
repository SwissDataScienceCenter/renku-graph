package io.renku.jsonld.generators

import io.renku.jsonld.{EntityId, EntityType, Schema}
import org.scalacheck.Gen
import Generators._

object JsonLDGenerators {

  val absoluteUriEntityIds: Gen[EntityId] = httpUrls map EntityId.fromAbsoluteUri
  val relativeUriEntityIds: Gen[EntityId] = relativePaths() map EntityId.fromRelativeUri

  implicit val entityIds: Gen[EntityId] = Gen.oneOf(
    absoluteUriEntityIds,
    relativeUriEntityIds
  )

  implicit val schemas: Gen[Schema] = for {
    baseUrl <- httpUrls
    path    <- relativePaths(maxSegments = 3)
  } yield Schema.from(s"$baseUrl/$path")

  implicit val entityTypes: Gen[EntityType] = for {
    schema   <- schemas
    property <- nonBlankStrings()
  } yield EntityType.fromProperty(schema / property.value)
}
