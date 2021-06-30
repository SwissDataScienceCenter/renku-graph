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

  private def parseObject(jsonObject: JsonObject): Either[ParsingFailure, JsonLD] = {
    val propsMap = jsonObject.toMap
    (propsMap.get("@id"), propsMap.get("@type"), propsMap.get("@value")) match {
      case (Some(entityId), Some(types), None) => jsonObject.toJsonLdEntity(entityId, types)
      case (Some(entityId), None, None)        => toJsonLDEntityId(entityId)
      case (None, maybeTypes, Some(value))     => toJsonLDValue(maybeTypes, value)
      case (None, _, _)                        => ParsingFailure("Entity without @id").asLeft[JsonLD]
      case (Some(_), Some(_), Some(_))         => ParsingFailure("Invalid entity").asLeft[JsonLD]
      case (Some(_), None, Some(_))            => ParsingFailure("Entity with @id and @value but no @type").asLeft[JsonLD]
    }
  }

  private implicit class JsonObjectOps(jsonObject: JsonObject) {

    def toJsonLdEntity(entityId: Json, types: Json) = for {
      id         <- entityId.as[EntityId].leftMap(e => ParsingFailure(e.message, e))
      types      <- types.as[EntityTypes].leftMap(e => ParsingFailure(e.message, e))
      properties <- extractProperties(jsonObject)
      // TODO check for @reverse
    } yield JsonLD.JsonLDEntity(id, types, properties.toMap, Reverse.empty)

    private def extractProperties(jsonObject: JsonObject) =
      jsonObject.toMap.view
        .filterKeys(key => key != "@id" && key != "@type")
        .toList
        .map(toPropsAndValues)
        .sequence

    private lazy val toPropsAndValues: ((String, Json)) => Either[ParsingFailure, (Property, JsonLD)] = {
      case (prop, value) if value.isObject =>
        value.asObject
          .map(parseObject(_).map(Property(prop) -> _))
          .getOrElse(ParsingFailure(s"Malformed entity's $prop property value").asLeft[(Property, JsonLD)])
      case (prop, value) if value.isArray =>
        val failure = ParsingFailure(s"Malformed entity's $prop array property value")
        for {
          jsons       <- value.asArray.map(_.asRight[ParsingFailure]).getOrElse(failure.asLeft[Vector[Json]])
          jsonObjects <- jsons.map(_.asObject.map(_.asRight).getOrElse(failure.asLeft[JsonObject])).sequence
          propValues  <- jsonObjects.map(parseObject).sequence
        } yield Property(prop) -> JsonLD.arr(propValues: _*)
      case (prop, _) => ParsingFailure(s"Malformed entity's $prop property value").asLeft[(Property, JsonLD)]
    }
  }

  private def toJsonLDEntityId(entityId: Json) =
    entityId
      .as[EntityId]
      .bimap(ParsingFailure(s"Could not parse @id: $entityId", _), JsonLD.fromEntityId)

  private def toJsonLDValue(maybeTypes: Option[Json], value: Json) = {

    def valueToJsonLDValue(maybeTypes: Option[EntityTypes]): JsonLD => Either[ParsingFailure, JsonLD] = {
      case jsonLDValue: JsonLDValue[_] =>
        jsonLDValue.value match {
          case number:  JsonNumber => JsonLDValue(number, maybeTypes).asRight[ParsingFailure]
          case boolean: Boolean    => JsonLDValue(boolean, maybeTypes).asRight[ParsingFailure]
          case string:  String     => stringToJsonLDValue(string)(maybeTypes)
          case t => ParsingFailure(s"Unrecognized type ${t.getClass} for json ld value").asLeft[JsonLD]
        }
      case _ => ParsingFailure("Could not parse json ld value").asLeft[JsonLD]
    }

    def stringToJsonLDValue(string: String): Option[EntityTypes] => Either[ParsingFailure, JsonLD] = {
      case Some(JsonLDInstantValue.entityTypes) =>
        Try(Instant.parse(string)).toEither.bimap(
          e => ParsingFailure(s"Could not parse $string to instant", e),
          JsonLDInstantValue.from
        )
      case Some(JsonLDLocalDateValue.entityTypes) =>
        Try(LocalDate.parse(string)).toEither.bimap(
          e => ParsingFailure(s"Could not parse $string to local date", e),
          JsonLDLocalDateValue.from
        )
      case maybeTypes => JsonLDValue(string, maybeTypes).asRight[ParsingFailure]
    }

    for {
      value       <- parse(value)
      maybeTypes  <- maybeTypes.map(_.as[EntityTypes].leftMap(e => ParsingFailure(e.message, e))).sequence
      jsonLDValue <- valueToJsonLDValue(maybeTypes)(value)
    } yield jsonLDValue
  }
}
