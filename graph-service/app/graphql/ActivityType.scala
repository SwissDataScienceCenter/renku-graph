package graphql

import models.Activity
import sangria.execution.deferred.{ Fetcher, HasId }
import sangria.schema.{ Field, ListType, ObjectType, StringType, fields }

object ActivityType {

  implicit val ActivityType: ObjectType[Unit, Activity] = {
    ObjectType(
      "Activity",
      "A renku activity",

      () => fields[Unit, Activity](
        Field( "id", StringType, resolve = _.value.id ),
        Field( "label", StringType, resolve = _.value.label ),
        Field( "endTime", graphql.InstantType, resolve = _.value.endTime ),
        Field(
          "generationEdges",
          ListType( GenerationEdgeType.GenerationEdgeType ),
          resolve = c => GenerationEdgeType.generationEdges.deferRelSeq( GenerationEdgeType.byGeneratingActivity, c.value.id )
        )
      )
    )
  }

  lazy val activities: Fetcher[UserContext, Activity, Activity, String] =
    Fetcher( ( ctx: UserContext, ids: Seq[String] ) =>
      ctx.dal.highLevel.activities( ids ) )( HasId( _.id ) )

}
