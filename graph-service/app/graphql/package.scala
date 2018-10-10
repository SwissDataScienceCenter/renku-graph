import java.time.format.DateTimeFormatter
import java.time.{ Instant, ZoneId }

import persistence.DatabaseAccessLayer
import sangria.execution.deferred.DeferredResolver
import sangria.schema.{ Field, ListType, ObjectType, ScalarType, Schema, fields }
import sangria.validation.ValueCoercionViolation

import scala.util.{ Failure, Success, Try }

package object graphql {
  lazy val QueryType: ObjectType[DatabaseAccessLayer, Unit] = ObjectType(
    "Query",
    fields[DatabaseAccessLayer, Unit](
      Field(
        "activities",
        ListType( ActivityType.ActivityType ),
        description = Some( "Returns a list of all activities" ),
        resolve = _.ctx.highLevel.activities
      ),
      Field(
        "entities",
        ListType( EntityType.EntityType ),
        description = Some( "Returns a list of all entities" ),
        resolve = _.ctx.highLevel.entities
      )
    )
  )

  lazy val schema: Schema[DatabaseAccessLayer, Unit] = Schema( QueryType )

  lazy val resolver: DeferredResolver[DatabaseAccessLayer] = {
    DeferredResolver.fetchers(
      ActivityType.activities,
      EntityType.entities,
      GenerationEdgeType.generationEdges
    )
  }

  implicit val InstantType: ScalarType[Instant] = ScalarType[Instant](
    "Instant",
    coerceOutput    = ( d, _ ) =>
      d.atZone( ZoneId.of( "UTC" ) ).format( DateTimeFormatter.ISO_INSTANT ),
    coerceUserInput = {
      case s: String => parseInstant( s )
      case _         => Left( InstantCoercionViolation )
    },
    coerceInput     = {
      case sangria.ast.StringValue( s, _, _, _, _ ) => parseInstant( s )
      case _                                        => Left( InstantCoercionViolation )
    }
  )

  case object InstantCoercionViolation
    extends ValueCoercionViolation( "Instant value expected" )

  def parseInstant( s: String ): Either[InstantCoercionViolation.type, Instant] =
    Try( Instant.parse( s ) ) match {
      case Success( value ) => Right( value )
      case Failure( _ ) =>
        Try( Instant.parse( s"${s}Z" ) ) match {
          case Success( value ) => Right( value )
          case Failure( _ )     => Left( InstantCoercionViolation )
        }
    }
}
