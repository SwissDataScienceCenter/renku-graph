package graphql

import models.Activity
import persistence.DatabaseAccessLayer
import sangria.execution.deferred.{ Fetcher, HasId }
import sangria.macros.derive._
import sangria.schema.{ DeferredValue, Field, ListType, ObjectType, StringType, fields }

object ActivityType {

  import graphql.InstantType

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

    //    deriveObjectType[Unit, Activity](
    //      ObjectTypeDescription( "A renku activity" ),
    //      ReplaceField(
    //        "endTime",
    //        Field(
    //          "endTime",
    //          graphql.InstantType,
    //          resolve = _.value.endTime
    //        )
    //      )
    //    ,
    //      AddFields(
    //        Field(
    //          "generationEdges",
    //          ListType( GenerationEdgeType.GenerationEdgeType ),
    //          resolve = c => GenerationEdgeType.generationEdges.deferRelSeq( GenerationEdgeType.byGeneratingActivity, c.value.id )
    //        )
    //      )
    //    )
  }

  lazy val activities: Fetcher[DatabaseAccessLayer, Activity, Activity, String] =
    Fetcher( ( ctx: DatabaseAccessLayer, ids: Seq[String] ) =>
      ctx.highLevel.activities( ids ) )( HasId( _.id ) )

}
