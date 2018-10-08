package graphql

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import models._
import sangria.macros.derive._
import sangria.schema.{ObjectType, ScalarType}
import sangria.validation.ValueCoercionViolation

import scala.util.{Failure, Success, Try}

object SchemaTypes {

  implicit val InstantType: ScalarType[Instant] = ScalarType[Instant](
    "Instant",
    coerceOutput = (d, _) =>
      d.atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT),
    coerceUserInput = {
      case s: String => parseInstant(s)
      case _         => Left(InstantCoercionViolation)
    },
    coerceInput = {
      case sangria.ast.StringValue(s, _, _, _, _) => parseInstant(s)
      case _                                      => Left(InstantCoercionViolation)
    }
  )

  implicit val ActivityType: ObjectType[Unit, Activity] =
    deriveObjectType[Unit, Activity](
      ObjectTypeDescription("A renku activity"),
    )

  implicit val EntityType: ObjectType[Unit, Entity] =
    deriveObjectType[Unit, Entity](
      ObjectTypeDescription("A renku entity"),
    )

  implicit val PersonType: ObjectType[Unit, Person] =
    deriveObjectType[Unit, Person](
      ObjectTypeDescription("A renku person"),
    )

  implicit val ProjectType: ObjectType[Unit, Project] =
    deriveObjectType[Unit, Project](
      ObjectTypeDescription("A renku project"),
    )

  case object InstantCoercionViolation
      extends ValueCoercionViolation("Instant value expected")

  def parseInstant(s: String): Either[InstantCoercionViolation.type, Instant] =
    Try(Instant.parse(s)) match {
      case Success(value) => Right(value)
      case Failure(_)     => Left(InstantCoercionViolation)
    }

}
