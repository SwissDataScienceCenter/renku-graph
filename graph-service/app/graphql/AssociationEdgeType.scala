package graphql

import models.AssociationEdge
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.macros.derive._
import sangria.schema.{ Field, ObjectType }

import scala.concurrent.ExecutionContext

object AssociationEdgeType {

  //  implicit val GenerationEdgeType: ObjectType[Unit, AssociationEdge] = {
  //    deriveObjectType[Unit, AssociationEdge](
  //      ObjectTypeDescription( "A renku association edge" ),
  //      AddFields(
  //        Field( "activity", ActivityType.ActivityType, resolve = c => ActivityType.activities.defer( c.value.activityId ) ),
  //        Field( "agent", ???.EntityType, resolve = c => EntityType.entities.defer( c.value.entityId ) ),
  //        Field( "plan", EntityType.EntityType, resolve = c => EntityType.entities.defer( c.value.entityId ) )
  //      )
  //    )
  //  }
  //
  //  lazy val generationEdges: Fetcher[UserContext, AssociationEdge, AssociationEdge, ( String, String )] = Fetcher.relOnly(
  //    ( ctx: UserContext, ids: RelationIds[AssociationEdge] ) => {
  //      implicit val ec: ExecutionContext = ctx.dal.ec
  //      for {
  //        seq1 <- ctx.dal.generations.api.findByActivity( ids( byGeneratingActivity ) )
  //        seq2 <- ctx.dal.generations.api.findByEntity( ids( byGeneratedEntity ) )
  //      } yield seq1.map( _._1 ) ++ seq2.map( _._1 )
  //    }
  //  )( HasId( ( edge: AssociationEdge ) => ( edge.entityId, edge.activityId ) ) )
  //
  //  lazy val byGeneratedEntity: Relation[AssociationEdge, AssociationEdge, String] = Relation[AssociationEdge, String](
  //    "byGeneratedEntity",
  //    edge => Seq( edge.entityId )
  //  )
  //
  //  lazy val byGeneratingActivity: Relation[AssociationEdge, AssociationEdge, String] = Relation[AssociationEdge, String](
  //    "byGeneratingActivity",
  //    edge => Seq( edge.activityId )
  //  )

}
