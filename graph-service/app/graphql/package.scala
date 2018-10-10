import persistence.DatabaseAccessLayer
import sangria.schema.{ Field, ListType, ObjectType, Schema, fields }

package object graphql {
  val QueryType: ObjectType[DatabaseAccessLayer, Unit] = ObjectType(
    "Query",
    fields[DatabaseAccessLayer, Unit](
      Field(
        "activities",
        ListType( SchemaTypes.ActivityType ),
        description = Some( "Returns a list of all activities" ),
        resolve = _.ctx.highLevel.activities
      )
    )
  )

  val schema: Schema[DatabaseAccessLayer, Unit] = Schema( QueryType )
}
