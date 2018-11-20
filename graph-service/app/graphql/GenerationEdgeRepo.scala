package graphql

import models.GenerationEdge
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.macros.derive._
import sangria.schema.{ Field, ObjectType }

import scala.concurrent.ExecutionContext

object GenerationEdgeRepo {

  implicit lazy val GenerationEdgeType: ObjectType[Unit, GenerationEdge] = {
    deriveObjectType[Unit, GenerationEdge](
      ObjectTypeDescription( "A renku generation edge" ),
      AddFields(
        Field( "entity", EntityRepo.EntityType, resolve = c => EntityRepo.entities.defer( c.value.entityId ) ),
        Field( "activity", ActivityRepo.ActivityType, resolve = c => ActivityRepo.activities.defer( c.value.activityId ) )
      )
    )
  }

  lazy val generationEdges: Fetcher[UserContext, GenerationEdge, GenerationEdge, ( String, String )] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[GenerationEdge] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq1 <- ctx.dal.generations.api.findByEntity( ids( byGeneratedEntity ) )
          seq2 <- ctx.dal.generations.api.findByActivity( ids( byGeneratingActivity ) )
        } yield seq1.map( _._1 ) ++ seq2.map( _._1 )
      }
    )( HasId( ( edge: GenerationEdge ) => ( edge.entityId, edge.activityId ) ) )

  lazy val byGeneratedEntity: Relation[GenerationEdge, GenerationEdge, String] =
    Relation(
      "byGeneratedEntity",
      edge => Seq( edge.entityId )
    )

  lazy val byGeneratingActivity: Relation[GenerationEdge, GenerationEdge, String] =
    Relation(
      "byGeneratingActivity",
      edge => Seq( edge.activityId )
    )

}
