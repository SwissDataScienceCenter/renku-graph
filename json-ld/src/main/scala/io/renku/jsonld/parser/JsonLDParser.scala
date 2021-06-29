package io.renku.jsonld.parser
import cats.syntax.all._
import io.circe.{Json, JsonNumber, JsonObject}
import io.renku.jsonld.JsonLD.{JsonLDInstantValue, JsonLDLocalDateValue, JsonLDValue}
import io.renku.jsonld._

import java.time.{Instant, LocalDate}
import scala.util.Try

private class JsonLDParser() extends Parser {
  override def parse(string: String): Either[ParsingFailure, JsonLD] =
    io.circe.parser.parse(string).leftMap(e => ParsingFailure(e.message, e)).flatMap(parse)

  override def parse(json: Json): Either[ParsingFailure, JsonLD] = json.fold(
    jsonNull = JsonLD.Null.asRight[ParsingFailure],
    jsonBoolean = JsonLD.fromBoolean(_).asRight[ParsingFailure],
    jsonNumber = JsonLD.fromNumber(_).asRight[ParsingFailure],
    jsonString = JsonLD.fromString(_).asRight[ParsingFailure],
    jsonArray = _.map(parse).toList.sequence.map(JsonLD.arr(_: _*)),
    jsonObject = parseObject
  )

  private def parseObject(jsonObject: JsonObject): Either[ParsingFailure, JsonLD] =
    (jsonObject.toMap.get("@id"), jsonObject.toMap.get("@type"), jsonObject.toMap.get("@value")) match {
      case (Some(idAsJson), Some(typesAsJson), None) =>
        for {
          id    <- idAsJson.as[EntityId].leftMap(e => ParsingFailure(e.message, e))
          types <- typesAsJson.as[EntityTypes].leftMap(e => ParsingFailure(e.message, e))
          properties <- jsonObject.toMap.view
                          .filterKeys(key => key != "@id" && key != "@type")
                          .toList
                          .map { case (k, v) =>
                            parse(v).map(Property(k) -> _)
                          }
                          .sequence
          // TODO check for @reverse
        } yield JsonLD.JsonLDEntity(id, types, properties.toMap, Reverse.empty)
      case (Some(entityId), None, None) =>
        entityId
          .as[EntityId]
          .bimap(e => ParsingFailure(s"Could not parse entity id: $entityId", e), JsonLD.fromEntityId)
      case (None, maybeTypesAsJson, Some(valueAsJson)) =>
        for {
          value       <- parse(valueAsJson)
          maybeTypes  <- maybeTypesAsJson.map(_.as[String].leftMap(e => ParsingFailure(e.message, e))).sequence
          jsonLDValue <- valueToJsonLDValue(maybeTypes)(value)
        } yield jsonLDValue

      case (None, _, _)                => ParsingFailure("Entity without @id").asLeft[JsonLD]
      case (Some(_), Some(_), Some(_)) => ParsingFailure("Invalid entity").asLeft[JsonLD]
      case (Some(_), None, Some(_))    => ParsingFailure("Entity with @id and @value but no @type").asLeft[JsonLD]
    }

  private def valueToJsonLDValue(maybeTypes: Option[String]): JsonLD => Either[ParsingFailure, JsonLD] = {
    case jsonLDValue: JsonLDValue[_] =>
      jsonLDValue.value match {
        case number:  JsonNumber => JsonLDValue(number, maybeTypes).asRight[ParsingFailure]
        case boolean: Boolean    => JsonLDValue(boolean, maybeTypes).asRight[ParsingFailure]
        case string:  String     => stringToJsonLDValue(string)(maybeTypes)
        case _ => ParsingFailure("Unrecognized type for json ld value").asLeft[JsonLD]
      }
    case _ => ParsingFailure("Could not parse json ld value").asLeft[JsonLD]
  }

  private def stringToJsonLDValue(string: String): Option[String] => Either[ParsingFailure, JsonLD] = {
    case Some(entityType) if entityType == JsonLDInstantValue.entityType.show =>
      Try(Instant.parse(string)).toEither.bimap(
        e => ParsingFailure(s"Could not parse $string to instant", e),
        JsonLDInstantValue.from
      )
    case Some(entityType) if entityType == JsonLDLocalDateValue.entityType.show =>
      Try(LocalDate.parse(string)).toEither.bimap(
        e => ParsingFailure(s"Could not parse $string to local date", e),
        JsonLDLocalDateValue.from
      )
    case maybeTypes => JsonLDValue(string, maybeTypes).asRight[ParsingFailure]
  }
}
