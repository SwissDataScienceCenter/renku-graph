package graphql

import models.AssociationEdge
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.macros.derive._
import sangria.schema.{ Field, ObjectType }

import scala.concurrent.ExecutionContext

object AssociationEdgeRepo {

  implicit lazy val AssociationEdgeType: ObjectType[Unit, AssociationEdge] = {
    deriveObjectType[Unit, AssociationEdge](
      ObjectTypeDescription( "A renku association edge" ),
      AddFields(
        Field( "activity", ActivityRepo.ActivityType, resolve = c => ActivityRepo.activities.defer( c.value.activityId ) ),
        Field( "agent", PersonRepo.PersonType, resolve = c => PersonRepo.persons.defer( c.value.agentId ) ),
        Field( "hadPlan", EntityRepo.EntityType, resolve = c => EntityRepo.entities.defer( c.value.planId ) )
      )
    )
  }

  lazy val associationEdges: Fetcher[UserContext, AssociationEdge, AssociationEdge, ( String, String )] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[AssociationEdge] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq1 <- ctx.dal.associations.api.findByActivity( ids( byAssociatedActivity ) )
          seq2 <- ctx.dal.associations.api.findByAgent( ids( byAssociatedAgent ) )
          seq3 <- ctx.dal.associations.api.findByPlan( ids( byAssociatedPlan ) )
        } yield seq1.map( _._1 ) ++ seq2.map( _._1 ) ++ seq3.map( _._1 )
      }
    )( HasId( ( edge: AssociationEdge ) => ( edge.activityId, edge.agentId ) ) )

  lazy val byAssociatedActivity: Relation[AssociationEdge, AssociationEdge, String] =
    Relation(
      "byAssociatedActivity",
      edge => Seq( edge.activityId )
    )

  lazy val byAssociatedAgent: Relation[AssociationEdge, AssociationEdge, String] =
    Relation(
      "byAssociatedAgent",
      edge => Seq( edge.agentId )
    )

  lazy val byAssociatedPlan: Relation[AssociationEdge, AssociationEdge, String] =
    Relation(
      "byAssociatedPlan",
      edge => Seq( edge.planId )
    )

}
