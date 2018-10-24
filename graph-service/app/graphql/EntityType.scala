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
          "qualifiedGeneration",
          ListType( GenerationEdgeType.GenerationEdgeType ),
          resolve = c => GenerationEdgeType.generationEdges.deferRelSeq( GenerationEdgeType.byGeneratedEntity, c.value.id )
        ),
        Field(
          "wasGeneratedBy",
          ListType( ActivityType.ActivityType ),
          resolve = c => ActivityType.activitiesFromGeneration.deferRelSeq( ActivityType.byGeneratedEntity, c.value.id )
        )
      )
    )
  }

  lazy val entities: Fetcher[UserContext, Entity, Entity, String] =
    Fetcher( ( ctx: UserContext, ids: Seq[String] ) =>
      ctx.dal.entities.api.find( ids ) )( HasId( _.id ) )

}
