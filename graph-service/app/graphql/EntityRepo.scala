package graphql

import models.{ AssociationEdge, Entity, GenerationEdge, UsageEdge }
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.macros.derive._
import sangria.schema.{ Field, ListType, ObjectType }

import scala.concurrent.ExecutionContext

object EntityRepo {

  implicit lazy val EntityType: ObjectType[Unit, Entity] = {
    deriveObjectType[Unit, Entity](
      ObjectTypeDescription( "A renku entity" ),
      AddFields(
        Field(
          "invHadPlanActivity",
          ListType( ActivityRepo.ActivityType ),
          resolve = c => ActivityRepo.activitiesFromAssociation.deferRelSeq( ActivityRepo.byAssociatedPlan, c.value.id )
        ),
        Field(
          "invHadPlanAgent",
          ListType( PersonRepo.PersonType ),
          resolve = c => PersonRepo.personsFromAssociation.deferRelSeq( PersonRepo.byAssociatedPlan, c.value.id )
        ),
        Field(
          "invHadPlan",
          ListType( AssociationEdgeRepo.AssociationEdgeType ),
          resolve = c => AssociationEdgeRepo.associationEdges.deferRelSeq( AssociationEdgeRepo.byAssociatedPlan, c.value.id )
        ),
        Field(
          "qualifiedGeneration",
          ListType( GenerationEdgeRepo.GenerationEdgeType ),
          resolve = c => GenerationEdgeRepo.generationEdges.deferRelSeq( GenerationEdgeRepo.byGeneratedEntity, c.value.id )
        ),
        Field(
          "wasGeneratedBy",
          ListType( ActivityRepo.ActivityType ),
          resolve = c => ActivityRepo.activitiesFromGeneration.deferRelSeq( ActivityRepo.byGeneratedEntity, c.value.id )
        ),
        Field(
          "invQualifiedUsage",
          ListType( UsageEdgeRepo.UsageEdgeType ),
          resolve = c => UsageEdgeRepo.usageEdges.deferRelSeq( UsageEdgeRepo.byUsedEntity, c.value.id )
        ),
        Field(
          "invUsed",
          ListType( ActivityRepo.ActivityType ),
          resolve = c => ActivityRepo.activitiesFromUsage.deferRelSeq( ActivityRepo.byUsedEntity, c.value.id )
        )
      )
    )
  }

  lazy val entities: Fetcher[UserContext, Entity, Entity, String] =
    Fetcher( ( ctx: UserContext, ids: Seq[String] ) =>
      ctx.dal.entities.api.find( ids ) )( HasId( _.id ) )

  lazy val entitiesFromGeneration: Fetcher[UserContext, Entity, ( GenerationEdge, Entity ), String] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[Entity] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq <- ctx.dal.generations.api.findByActivity( ids( byGeneratingActivity ) )
        } yield seq
      }
    )( HasId( _.id ) )

  lazy val byGeneratingActivity: Relation[Entity, ( GenerationEdge, Entity ), String] =
    Relation(
      "byGeneratingEntity",
      x => Seq( x._1.activityId ),
      x => x._2
    )

  lazy val entitiesFromUsage: Fetcher[UserContext, Entity, ( UsageEdge, Entity ), String] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[Entity] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq <- ctx.dal.usages.api.findByActivity( ids( byUsingActivity ) )
        } yield seq
      }
    )( HasId( _.id ) )

  lazy val byUsingActivity: Relation[Entity, ( UsageEdge, Entity ), String] =
    Relation(
      "byUsingActivity",
      x => Seq( x._1.activityId ),
      x => x._2
    )

  lazy val entitiesFromAssociation: Fetcher[UserContext, Entity, ( AssociationEdge, Entity ), String] =
    Fetcher.relOnly(
      ( ctx: UserContext, ids: RelationIds[Entity] ) => {
        implicit val ec: ExecutionContext = ctx.dal.ec
        for {
          seq1 <- ctx.dal.associations.api.findByActivity( ids( byAssociatedActivity ) )
          seq2 <- ctx.dal.associations.api.findByAgent( ids( byAssociatedAgent ) )
        } yield seq1.map( x => ( x._1, x._3 ) ) ++ seq2.map( x => ( x._1, x._3 ) )
      }
    )( HasId( _.id ) )

  lazy val byAssociatedActivity: Relation[Entity, ( AssociationEdge, Entity ), String] =
    Relation(
      "byAssociatedActivity",
      x => Seq( x._1.activityId ),
      x => x._2
    )

  lazy val byAssociatedAgent: Relation[Entity, ( AssociationEdge, Entity ), String] =
    Relation(
      "byAssociatedAgent",
      x => Seq( x._1.agentId ),
      x => x._2
    )

}
