package graphql

import models.GenerationEdge
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.macros.derive._
import sangria.schema.{ Field, ObjectType }

import scala.concurrent.ExecutionContext

object GenerationEdgeType {

  implicit val GenerationEdgeType: ObjectType[Unit, GenerationEdge] = {
    deriveObjectType[Unit, GenerationEdge](
      ObjectTypeDescription( "A renku generation edge" ),
      AddFields(
        Field( "activity", ActivityType.ActivityType, resolve = c => ActivityType.activities.defer( c.value.activityId ) ),
        Field( "entity", EntityType.EntityType, resolve = c => EntityType.entities.defer( c.value.entityId ) )
      )
    )
  }

  lazy val generationEdges: Fetcher[UserContext, GenerationEdge, GenerationEdge, ( String, String )] = Fetcher.relOnly(
    ( ctx: UserContext, ids: RelationIds[GenerationEdge] ) => {
      implicit val ec: ExecutionContext = ctx.dal.ec
      for {
        seq1 <- ctx.dal.highLevel.generationEdgesByActivities( ids( byGeneratingActivity ) )
        seq2 <- ctx.dal.highLevel.generationEdgesByEntities( ids( byGeneratedEntity ) )
      } yield seq1 ++ seq2
    }
  )( HasId( ( edge: GenerationEdge ) => ( edge.entityId, edge.activityId ) ) )

  lazy val byGeneratedEntity: Relation[GenerationEdge, GenerationEdge, String] = Relation[GenerationEdge, String](
    "byGeneratedEntity",
    edge => Seq( edge.entityId )
  )

  lazy val byGeneratingActivity: Relation[GenerationEdge, GenerationEdge, String] = Relation[GenerationEdge, String](
    "byGeneratingActivity",
    edge => Seq( edge.activityId )
  )

}
