package graphql

import models.{ Activity, Entity }
import persistence.DatabaseAccessLayer
import sangria.execution.deferred.{ Fetcher, HasId }
import sangria.macros.derive._
import sangria.schema.{ Field, ListType, ObjectType }
import sangria.schema.fields
import sangria.schema.StringType

object EntityType {

  implicit val EntityType: ObjectType[Unit, Entity] = {
    deriveObjectType[Unit, Entity](
      ObjectTypeDescription( "A renku entity" ),
      AddFields(
        Field(
          "generationEdges",
          ListType( GenerationEdgeType.GenerationEdgeType ),
          resolve = c => GenerationEdgeType.generationEdges.deferRelSeq( GenerationEdgeType.byGeneratedEntity, c.value.id )
        )
      )
    )
  }

  lazy val entities: Fetcher[DatabaseAccessLayer, Entity, Entity, String] =
    Fetcher( ( ctx: DatabaseAccessLayer, ids: Seq[String] ) =>
      ctx.highLevel.entities( ids ) )( HasId( _.id ) )

}
