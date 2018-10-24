package graphql

import models.{ Activity, GenerationEdge }
import sangria.execution.deferred.{ Fetcher, HasId, Relation, RelationIds }
import sangria.schema.{ Field, ListType, ObjectType, StringType, fields }

import scala.concurrent.ExecutionContext

object ActivityType {

  implicit val ActivityType: ObjectType[Unit, Activity] = {
    ObjectType(
      "Activity",
      "A renku activity",

      () => fields[Unit, Activity](
        Field( "id", StringType, resolve = _.value.id ),
        Field( "label", StringType, resolve = _.value.label ),
        Field( "endTime", graphql.InstantType, resolve = _.value.endTime )
      //        Field(
      //          "generationEdges",
      //          ListType( GenerationEdgeType.GenerationEdgeType ),
      //          resolve = c => GenerationEdgeType.generationEdges.deferRelSeq( GenerationEdgeType.byGeneratingActivity, c.value.id )
      //        )
      )
    )
  }

  lazy val activities: Fetcher[UserContext, Activity, Activity, String] =
    Fetcher( ( ctx: UserContext, ids: Seq[String] ) =>
      ctx.dal.activities.api.find( ids ) )( HasId( _.id ) )

  lazy val activitiesFromGeneration = Fetcher.relOnly[UserContext, Activity, ( GenerationEdge, Activity ), String](
    ( ctx: UserContext, ids: RelationIds[Activity] ) => {
      implicit val ec: ExecutionContext = ctx.dal.ec
      for {
        seq <- ctx.dal.generations.api.findByEntity( ids( byGeneratedEntity ) )
      } yield seq
    }
  )( HasId( _.id ) )

  lazy val byGeneratedEntity: Relation[Activity, ( GenerationEdge, Activity ), String] = Relation[Activity, ( GenerationEdge, Activity ), String](
    "byGeneratedEntity",
    x => Seq( x._1.entityId ),
    x => x._2
  )

}
