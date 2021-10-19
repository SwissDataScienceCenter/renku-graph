/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.jsonld.parser

import cats.syntax.all._
import io.circe.{Json, JsonNumber, JsonObject}
import io.renku.jsonld.JsonLD.{JsonLDInstantValue, JsonLDLocalDateValue, JsonLDValue}
import io.renku.jsonld._

import java.time.{Instant, LocalDate, OffsetDateTime}
import scala.util.Try

private class JsonLDParser() extends Parser {

  override def parse(string: String): Either[ParsingFailure, JsonLD] =
    io.circe.parser.parse(string).leftMap(e => ParsingFailure(e.message, e)).flatMap(parse)

  override def parse(json: Json): Either[ParsingFailure, JsonLD] = json.fold(
    jsonNull = JsonLD.Null.asRight[ParsingFailure],
    jsonBoolean = JsonLD.fromBoolean(_).asRight[ParsingFailure],
    jsonNumber = JsonLD.fromNumber(_).asRight[ParsingFailure],
    jsonString = JsonLD.fromString(_).asRight[ParsingFailure],
    jsonArray = _.map(parse).toList.sequence
      .map(JsonLD.arr(_: _*))
      .flatMap(_.merge.leftMap(err => ParsingFailure(err.getMessage, err))),
    jsonObject = parseObject
  )

  private def parseObject(jsonObject: JsonObject): Either[ParsingFailure, JsonLD] = {
    val propsMap = jsonObject.toMap
    (propsMap.get("@id"), propsMap.get("@type"), propsMap.get("@reverse"), propsMap.get("@value")) match {
      case (Some(entityId), Some(types), maybeReverse, None) => jsonObject.toJsonLdEntity(entityId, types, maybeReverse)
      case (Some(entityId), None, None, None) if !propsMap.view.keys.exists(notReserved) => toJsonLDEntityId(entityId)
      case (Some(entityId), None, None, None)    => toJsonLDEdge(entityId, propsMap)
      case (None, maybeTypes, None, Some(value)) => toJsonLDValue(maybeTypes, value)
      case (Some(_), None, Some(_), None)        => ParsingFailure("Entity with reverse but no type").asLeft[JsonLD]
      case (None, _, _, _)                       => ParsingFailure("Entity without @id").asLeft[JsonLD]
      case (Some(_), Some(_), _, Some(_))        => ParsingFailure("Invalid entity").asLeft[JsonLD]
      case (Some(_), None, _, Some(_)) => ParsingFailure("Entity with @id and @value but no @type").asLeft[JsonLD]
    }
  }

  private implicit class JsonObjectOps(jsonObject: JsonObject) {

    def toJsonLdEntity(entityId:     Json,
                       types:        Json,
                       maybeReverse: Option[Json]
    ): Either[ParsingFailure, JsonLD.JsonLDEntity] = for {
      id         <- entityId.as[EntityId].leftMap(e => ParsingFailure(e.message, e))
      types      <- types.as[EntityTypes].leftMap(e => ParsingFailure(e.message, e))
      properties <- extractProperties(jsonObject)
      reverse    <- maybeReverse.map(extractReverse).getOrElse(Reverse.empty.asRight)
    } yield JsonLD.JsonLDEntity(id, types, properties.toMap, reverse)

    private def extractProperties(jsonObject: JsonObject) =
      jsonObject.toMap.view
        .filterKeys(key => key != "@id" && key != "@type" && key != "@reverse")
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

    private lazy val extractReverse: Json => Either[ParsingFailure, Reverse] = {
      case json if json.isObject =>
        json.asObject
          .map(parseReverseObject)
          .getOrElse(ParsingFailure("Malformed entity's @reverse property").asLeft[Reverse])
      case _ => ParsingFailure("Malformed entity's @reverse property - not an object").asLeft[Reverse]
    }

    private def parseReverseObject(jsonObject: JsonObject): Either[ParsingFailure, Reverse] =
      jsonObject.toMap.view
        .filterKeys(notReserved)
        .toList match {
        case Nil => ParsingFailure("Malformed entity's @reverse property - no properties defined").asLeft[Reverse]
        case props =>
          props
            .map(toPropAndJsonLD)
            .sequence
            .map(Reverse.fromList)
            .flatMap(_.fold(error => ParsingFailure(error.getMessage).asLeft[Reverse], _.asRight[ParsingFailure]))
      }

    private lazy val toPropAndJsonLD: ((String, Json)) => Either[ParsingFailure, (Property, JsonLD)] = {
      case (prop, json) => parse(json).map(Property(prop) -> _)
    }
  }

  private lazy val notReserved: String => Boolean =
    key => key != "@id" && key != "@type" && key != "@reverse" && key != "@value"

  private def toJsonLDEntityId(entityId: Json) = toEntityId(entityId).map(JsonLD.fromEntityId)

  private def toEntityId(entityId: Json) =
    entityId
      .as[EntityId]
      .bimap(ParsingFailure(s"Could not parse @id: $entityId", _), identity)

  private def toJsonLDEdge(entityId: Json, props: Map[String, Json]): Either[ParsingFailure, JsonLD] = {
    val propsWithoutId = props.view.filterKeys(_ != "@id")

    def extractId(id: EntityId)(json: Json) = json.asObject
      .flatMap(_.toMap.get("@id"))
      .map(toEntityId)
      .getOrElse(ParsingFailure(s"Edge $id has invalid target").asLeft)

    for {
      id <- toEntityId(entityId)
      _ <- if (propsWithoutId.size != 1) ParsingFailure(s"Edge $id with ${propsWithoutId.size} properties").asLeft
           else ().asRight[ParsingFailure]
      property <- Property(propsWithoutId.head._1).asRight
      targets <- propsWithoutId.head._2 match {
                   case target if target.isObject => extractId(id)(target).map(List(_))
                   case target if target.isArray =>
                     target.asArray.sequence.flatten.map(extractId(id)).toList.sequence
                   case _ => ParsingFailure(s"Edge $id has invalid target(s)").asLeft
                 }
    } yield targets match {
      case target :: Nil => JsonLD.edge(id, property, target)
      case targets       => JsonLD.arr(targets.map(JsonLD.edge(id, property, _)): _*)
    }
  }

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
        Either
          .catchNonFatal(Instant.parse(string))
          .orElse(Either.catchNonFatal(OffsetDateTime.parse(string).toInstant))
          .bimap(
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
