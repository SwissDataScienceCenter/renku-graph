package graphql

import models.Entity
import sangria.execution.deferred.{ Fetcher, HasId }
import sangria.macros.derive._
import sangria.schema.{ Field, ListType, ObjectType }

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

  lazy val entities: Fetcher[UserContext, Entity, Entity, String] =
    Fetcher( ( ctx: UserContext, ids: Seq[String] ) =>
      ctx.dal.highLevel.entities( ids ) )( HasId( _.id ) )

}
