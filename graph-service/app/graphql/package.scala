import java.time.format.DateTimeFormatter
import java.time.{ Instant, ZoneId }

import sangria.execution.deferred.DeferredResolver
import sangria.schema.{ Field, ListType, ObjectType, ScalarType, Schema, fields }
import sangria.validation.ValueCoercionViolation

import scala.util.{ Failure, Success, Try }

package object graphql {
  lazy val QueryType: ObjectType[UserContext, Unit] = ObjectType(
    "Query",
    fields[UserContext, Unit](
      Field(
        "activities",
        ListType( ActivityRepo.ActivityType ),
        description = Some( "Returns a list of all activities" ),
        resolve = _.ctx.dal.activities.api.all
      ),
      Field(
        "entities",
        ListType( EntityRepo.EntityType ),
        description = Some( "Returns a list of all entities" ),
        resolve = _.ctx.dal.entities.api.all
      ),
      Field(
        "users",
        ListType( PersonRepo.PersonType ),
        description = Some( "Returns a list of all users" ),
        resolve = _.ctx.dal.persons.api.all
      )
    )
  )

  lazy val schema: Schema[UserContext, Unit] = Schema( QueryType )

  lazy val resolver: DeferredResolver[UserContext] = {
    DeferredResolver.fetchers(
      ActivityRepo.activities,
      ActivityRepo.activitiesFromAssociation,
      ActivityRepo.activitiesFromGeneration,
      ActivityRepo.activitiesFromUsage,
      EntityRepo.entities,
      EntityRepo.entitiesFromAssociation,
      EntityRepo.entitiesFromGeneration,
      EntityRepo.entitiesFromUsage,
      PersonRepo.persons,
      PersonRepo.personsFromAssociation,
      AssociationEdgeRepo.associationEdges,
      GenerationEdgeRepo.generationEdges,
      UsageEdgeRepo.usageEdges
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
