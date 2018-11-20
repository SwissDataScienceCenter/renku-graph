package graphql

import models.{ Activity, AssociationEdge, GenerationEdge, UsageEdge }
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.schema.{ Field, ListType, ObjectType, StringType, fields }

import scala.concurrent.ExecutionContext

object ActivityRepo {

  implicit lazy val ActivityType: ObjectType[Unit, Activity] = {
    ObjectType(
      "Activity",
      "A renku activity",

      () => fields[Unit, Activity](
        Field( "id", StringType, resolve = _.value.id ),
        Field( "label", StringType, resolve = _.value.label ),
        Field( "endTime", graphql.InstantType, resolve = _.value.endTime ),
        Field(
          "wasAssociatedWith",
          ListType( PersonRepo.PersonType ),
          resolve = c => PersonRepo.personsFromAssociation.deferRelSeq( PersonRepo.byAssociatedActivity, c.value.id )
        ),
        Field(
          "qualifiedAssociationPlan",
          ListType( EntityRepo.EntityType ),
          resolve = c => EntityRepo.entitiesFromAssociation.deferRelSeq( EntityRepo.byAssociatedActivity, c.value.id )
        ),
        Field(
          "qualifiedAssociation",
          ListType( AssociationEdgeRepo.AssociationEdgeType ),
          resolve = c => AssociationEdgeRepo.associationEdges.deferRelSeq( AssociationEdgeRepo.byAssociatedActivity, c.value.id )
        ),
        Field(
          "invQualifiedGeneration",
          ListType( GenerationEdgeRepo.GenerationEdgeType ),
          resolve = c => GenerationEdgeRepo.generationEdges.deferRelSeq( GenerationEdgeRepo.byGeneratingActivity, c.value.id )
        ),
        Field(
          "invWasGeneratedBy",
          ListType( EntityRepo.EntityType ),
          resolve = c => EntityRepo.entitiesFromGeneration.deferRelSeq( EntityRepo.byGeneratingActivity, c.value.id )
        ),
        Field(
          "qualifiedUsage",
          ListType( UsageEdgeRepo.UsageEdgeType ),
          resolve = c => UsageEdgeRepo.usageEdges.deferRelSeq( UsageEdgeRepo.byUsingActivity, c.value.id )
        ),
        Field(
          "used",
          ListType( EntityRepo.EntityType ),
          resolve = c => EntityRepo.entitiesFromUsage.deferRelSeq( EntityRepo.byUsingActivity, c.value.id )
        )
      )
    )
  }

  lazy val activities: Fetcher[UserContext, Activity, Activity, String] =
    Fetcher( ( ctx: UserContext, ids: Seq[String] ) =>
      ctx.dal.activities.api.find( ids ) )( HasId( _.id ) )

  lazy val activitiesFromGeneration: Fetcher[UserContext, Activity, ( GenerationEdge, Activity ), String] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[Activity] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq <- ctx.dal.generations.api.findByEntity( ids( byGeneratedEntity ) )
        } yield seq
      }
    )( HasId( _.id ) )

  lazy val byGeneratedEntity: Relation[Activity, ( GenerationEdge, Activity ), String] =
    Relation(
      "byGeneratedEntity",
      x => Seq( x._1.entityId ),
      x => x._2
    )

  lazy val activitiesFromUsage: Fetcher[UserContext, Activity, ( UsageEdge, Activity ), String] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[Activity] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq <- ctx.dal.usages.api.findByEntity( ids( byUsedEntity ) )
        } yield seq
      }
    )( HasId( _.id ) )

  lazy val byUsedEntity: Relation[Activity, ( UsageEdge, Activity ), String] =
    Relation(
      "byUsedEntity",
      x => Seq( x._1.entityId ),
      x => x._2
    )

  lazy val activitiesFromAssociation: Fetcher[UserContext, Activity, ( AssociationEdge, Activity ), String] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[Activity] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq1 <- ctx.dal.associations.api.findByAgent( ids( byAssociatedAgent ) )
          seq2 <- ctx.dal.associations.api.findByPlan( ids( byAssociatedPlan ) )
        } yield seq1.map( x => ( x._1, x._2 ) ) ++ seq2.map( x => ( x._1, x._2 ) )
      }
    )( HasId( _.id ) )

  lazy val byAssociatedAgent: Relation[Activity, ( AssociationEdge, Activity ), String] =
    Relation(
      "byAssociatedAgent",
      x => Seq( x._1.agentId ),
      x => x._2
    )

  lazy val byAssociatedPlan: Relation[Activity, ( AssociationEdge, Activity ), String] =
    Relation(
      "byAssociatedPlan",
      x => Seq( x._1.planId ),
      x => x._2
    )

}
