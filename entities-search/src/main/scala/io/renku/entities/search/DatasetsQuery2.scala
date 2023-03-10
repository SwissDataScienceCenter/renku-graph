package io.renku.entities.search

import io.circe.Decoder
import io.renku.entities.search.Criteria.Filters
import model.Entity

object DatasetsQuery2 extends EntityQuery[Entity.Dataset] {
  override val entityType: Filters.EntityType = Filters.EntityType.Dataset
  override val selectVariables = Set(
    "?entityType",
    "?matchingScore",
    "?name",
    "?idsPathsVisibilities",
    "?sameAs",
    "?maybeDateCreated",
    "?maybeDatePublished",
    "?date",
    "?creatorsNames",
    "?maybeDescription",
    "?keywords",
    "?images"
  )

  override def query(criteria: Criteria): Option[String] = ???

  override def decoder[EE >: Entity.Dataset]: Decoder[EE] = ???
}
