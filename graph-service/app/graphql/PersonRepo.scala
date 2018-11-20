package graphql

import models.{ AssociationEdge, Person }
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.macros.derive._
import sangria.schema.{ Field, ListType, ObjectType }

import scala.concurrent.ExecutionContext

object PersonRepo {

  implicit lazy val PersonType: ObjectType[Unit, Person] = {
    deriveObjectType[Unit, Person](
      ObjectTypeDescription( "A renku person" ),
      AddFields(
        Field(
          "invWasAssociatedWith",
          ListType( ActivityRepo.ActivityType ),
          resolve = c => ActivityRepo.activitiesFromAssociation.deferRelSeq( ActivityRepo.byAssociatedAgent, c.value.id )
        ),
        Field(
          "invWasAssociatedWithPlan",
          ListType( EntityRepo.EntityType ),
          resolve = c => EntityRepo.entitiesFromAssociation.deferRelSeq( EntityRepo.byAssociatedAgent, c.value.id )
        ),
        Field(
          "invQualifiedAssociation",
          ListType( AssociationEdgeRepo.AssociationEdgeType ),
          resolve = c => AssociationEdgeRepo.associationEdges.deferRelSeq( AssociationEdgeRepo.byAssociatedAgent, c.value.id )
        )
      )
    )
  }

  lazy val persons: Fetcher[UserContext, Person, Person, String] =
    Fetcher( ( ctx: UserContext, ids: Seq[String] ) =>
      ctx.dal.persons.api.find( ids ) )( HasId( _.id ) )

  lazy val personsFromAssociation: Fetcher[UserContext, Person, ( AssociationEdge, Person ), String] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[Person] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq1 <- ctx.dal.associations.api.findByActivity( ids( byAssociatedActivity ) )
          seq2 <- ctx.dal.associations.api.findByPlan( ids( byAssociatedPlan ) )
        } yield seq1.map( x => ( x._1, x._2 ) ) ++ seq2.map( x => ( x._1, x._3 ) )
      }
    )( HasId( _.id ) )

  lazy val byAssociatedActivity: Relation[Person, ( AssociationEdge, Person ), String] =
    Relation(
      "byAssociatedActivity",
      x => Seq( x._1.activityId ),
      x => x._2
    )

  lazy val byAssociatedPlan: Relation[Person, ( AssociationEdge, Person ), String] =
    Relation(
      "byAssociatedPlan",
      x => Seq( x._1.planId ),
      x => x._2
    )

}
