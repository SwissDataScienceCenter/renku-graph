package graphql

import models.UsageEdge
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.macros.derive._
import sangria.schema.{ Field, ListType, ObjectType }

import scala.concurrent.ExecutionContext

object UsageEdgeRepo {

  implicit lazy val UsageEdgeType: ObjectType[Unit, UsageEdge] = {
    deriveObjectType[Unit, UsageEdge](
      ObjectTypeDescription( "A renku usage edge" ),
      AddFields(
        Field( "activity", ActivityRepo.ActivityType, resolve = c => ActivityRepo.activities.defer( c.value.activityId ) ),
        Field( "entity", EntityRepo.EntityType, resolve = c => EntityRepo.entities.defer( c.value.entityId ) )
      )
    )
  }

  lazy val usageEdges: Fetcher[UserContext, UsageEdge, UsageEdge, ( String, String )] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[UsageEdge] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq1 <- ctx.dal.usages.api.findByActivity( ids( byUsingActivity ) )
          seq2 <- ctx.dal.usages.api.findByEntity( ids( byUsedEntity ) )
        } yield seq1.map( _._1 ) ++ seq2.map( _._1 )
      }
    )( HasId( ( edge: UsageEdge ) => ( edge.activityId, edge.entityId ) ) )

  lazy val byUsingActivity: Relation[UsageEdge, UsageEdge, String] =
    Relation(
      "byUsingActivity",
      edge => Seq( edge.activityId )
    )

  lazy val byUsedEntity: Relation[UsageEdge, UsageEdge, String] =
    Relation(
      "byUsedEntity",
      edge => Seq( edge.entityId )
    )

}
