package graphql

import models.Person
import sangria.execution.deferred.{ Fetcher, HasId }
import sangria.macros.derive._
import sangria.schema.{ Field, ListType, ObjectType }

object PersonType {

  implicit val PersonType: ObjectType[Unit, Person] = {
    deriveObjectType[Unit, Person](
      ObjectTypeDescription( "A renku person" )
    )
  }

  lazy val persons: Fetcher[UserContext, Person, Person, String] =
    Fetcher( ( ctx: UserContext, ids: Seq[String] ) =>
      ctx.dal.persons.api.find( ids ) )( HasId( _.id ) )

}
