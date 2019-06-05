package ch.datascience.graphservice.lineage

import ch.datascience.graphservice.lineage.model._
import sangria.macros.derive._
import sangria.schema._

private object schema {

  private implicit val NodeType: ObjectType[Unit, Node] = ObjectType(
    name        = "node",
    description = "Lineage node",
    fields[Unit, Node](
      Field("id", StringType, Some("Node identifier"), resolve = _.value.id),
      Field("label", StringType, Some("Node label"), resolve   = _.value.label)
    )
  )

  private implicit val EdgeType: ObjectType[Unit, Edge] = ObjectType(
    name        = "edge",
    description = "Lineage edge",
    fields = fields[Unit, Edge](
      Field("id", StringType, Some("Edge identifier"), resolve = _.value.id)
    )
  )

  val LineageType: ObjectType[Unit, Lineage] = deriveObjectType[Unit, Lineage](
    ObjectTypeDescription("Lineage"),
    DocumentField("nodes", "Lineage nodes"),
    DocumentField("edges", "Lineage edges")
  )
}
