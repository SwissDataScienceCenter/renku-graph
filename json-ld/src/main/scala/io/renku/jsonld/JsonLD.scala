/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.jsonld

import java.io.Serializable
import java.time.{Instant, LocalDate}

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.{Encoder, Json}
import io.renku.jsonld.JsonLD.MalformedJsonLD

import scala.annotation.tailrec

abstract class JsonLD extends Product with Serializable {
  def toJson:      Json
  def entityId:    Option[EntityId]
  def entityTypes: Option[EntityTypes]
  def cursor: Cursor = Cursor.from(this)
  def flatten: Either[MalformedJsonLD, JsonLD]
}

object JsonLD {

  import io.circe.syntax._

  val Null: JsonLD = JsonLDNull

  def fromString(value:    String):    JsonLD = JsonLDValue(value)
  def fromInt(value:       Int):       JsonLD = JsonLDValue(value)
  def fromLong(value:      Long):      JsonLD = JsonLDValue(value)
  def fromInstant(value:   Instant):   JsonLD = JsonLDValue(value, "http://www.w3.org/2001/XMLSchema#dateTime")
  def fromLocalDate(value: LocalDate): JsonLD = JsonLDValue(value, "http://schema.org/Date")
  def fromBoolean(value:   Boolean): JsonLD = JsonLDValue(value)
  def fromOption[V](value: Option[V])(implicit encoder: JsonLDEncoder[V]): JsonLD = JsonLDOptionValue(value)
  def fromEntityId(id:     EntityId):  JsonLD = JsonLDEntityId(id)
  def arr(jsons:           JsonLD*):   JsonLD = JsonLDArray(jsons)

  def entity(
      id:            EntityId,
      types:         EntityTypes,
      firstProperty: (Property, JsonLD),
      other:         (Property, JsonLD)*
  ): JsonLDEntity = entity(id, types, reverse = Reverse.empty, firstProperty, other: _*)

  def entity(
      id:         EntityId,
      types:      EntityTypes,
      properties: NonEmptyList[(Property, JsonLD)],
      other:      (Property, JsonLD)*
  ): JsonLDEntity = JsonLDEntity(id, types, properties ++ other.toList, Reverse.empty)

  def entity(
      id:            EntityId,
      types:         EntityTypes,
      reverse:       Reverse,
      firstProperty: (Property, JsonLD),
      other:         (Property, JsonLD)*
  ): JsonLDEntity = JsonLDEntity(id, types, properties = NonEmptyList.of(firstProperty, other: _*), reverse)

  def entity(
      id:         EntityId,
      types:      EntityTypes,
      reverse:    Reverse,
      properties: NonEmptyList[(Property, JsonLD)]
  ): JsonLDEntity = JsonLDEntity(id, types, properties, reverse)

  private[jsonld] final case class JsonLDEntity(id:         EntityId,
                                                types:      EntityTypes,
                                                properties: NonEmptyList[(Property, JsonLD)],
                                                reverse:    Reverse
  ) extends JsonLD {

    override lazy val toJson: Json = Json.obj(
      List(
        "@id"   -> id.asJson,
        "@type" -> types.asJson
      ) ++ (properties.toList.map(toObjectProperties) :+ reverse.asProperty).flatten: _*
    )

    private lazy val toObjectProperties: ((Property, JsonLD)) => Option[(String, Json)] = {
      case (_, JsonLDNull)   => None
      case (property, value) => Some(property.url -> value.toJson)
    }

    private implicit class ReverseOps(reverse: Reverse) {
      lazy val asProperty: Option[(String, Json)] = reverse match {
        case Reverse.empty => None
        case other         => Some("@reverse" -> other.asJson)
      }
    }

    override lazy val entityId:    Option[EntityId]    = Some(id)
    override lazy val entityTypes: Option[EntityTypes] = Some(types)
    override lazy val flatten: Either[MalformedJsonLD, JsonLD] =
      deNest(properties, List.empty[JsonLDEntity]) match {
        case (Nil, _)                                      => Left(MalformedJsonLD("Empty property list"))
        case (prop :: props, entities) if entities.isEmpty => Right(this.copy(properties = NonEmptyList(prop, props)))
        case (prop :: props, entities) =>
          val groupedEntities = entities.groupBy(entity => entity.id)
          val idsReferToSameEntities: Boolean = groupedEntities.forall { case (_, entitiesPerId) =>
            entitiesPerId.forall(_ == entitiesPerId.head)
          }
          if (idsReferToSameEntities)
            Right(JsonLDArray(this.copy(properties = NonEmptyList(prop, props)) +: entities))
          else
            Left(MalformedJsonLD("Some entities share an ID even though they're not the same"))
      }

    //    @tailrec
//    def deNest(objectsToCheck: List[JsonLD], deNested: Map[EntityId, JsonLD]): Map[EntityId, JsonLD] = {
//       objectsToCheck.headOption match {
//          case Some(firstToCheck: JsonLDEntity) =>
    // check if such entityId is already in the deNested and raise the MalformedJsonLD if it is
//            val (updatedProperties, updatedObjectsToCheck) = firstToCheck.properties.foldLeft(List.empty[(Property, JsonLD)]) {
//              case (deNestedProperties, ((propertyName, entity: JsonLDEntity)) =>
//                (deNestedProperties :+ (propertyName -> entity.entityId.asJsonLD)) -> (objectsToCheck :+ entity)
//              case (deNestedProperties, ((propertyName,  JsonLDArray(jsons))) =>
//                val (idJsons, updatedObjectsToCheck) = jsons.foldLeft(List.empty[JsonLD] -> objectsToCheck) {
//                  case ((deNestedJsons, updatedObjectsToCheck), entity: JsonLDEntity) =>
//                    deNestedJsons :+ entity.entityId.asJsonLD
//                    updatedObjectsToCheck :+ entity
//                }
//                (deNestedProperties :+ (propertyName -> JsonLDArray(idJsons: _))) -> updatedObjectsToCheck
//              case (deNestedProperties, (propertyName, json)) =>
//                (deNestedProperties :+ (propertyName -> json)) -> objectsToCheck
//            }
//            deNest(updatedObjectsToCheck, deNested.put(firstToCheck.entityId -> firstToCheck.copy(properties = NonEmptyList.fromListUnsafe(updatedProperties))))
//          case Some(JsonLDArray(jsons)) =>
//            deNest(objectsToCheck :++ jsons, deNested)
//          case Some(firstToCheck) =>
//            // not so sure what to do here... Is that even possible? If yes, should we maybe put it to the deNested with some BlankNode?
//          case None => deNested
//       }
//    }

  }

  private[jsonld] final case class JsonLDValue[V](
      value:          V,
      maybeType:      Option[String] = None
  )(implicit encoder: Encoder[V])
      extends JsonLD {
    override lazy val toJson: Json = maybeType match {
      case None    => Json.obj("@value" -> value.asJson)
      case Some(t) => Json.obj("@type" -> t.asJson, "@value" -> value.asJson)
    }

    override lazy val entityId:    Option[EntityId]                = None
    override lazy val entityTypes: Option[EntityTypes]             = None
    override lazy val flatten:     Either[MalformedJsonLD, JsonLD] = this.asRight
  }

  private[jsonld] object JsonLDValue {
    def apply[V](value: V, entityType: String)(implicit encoder: Encoder[V]): JsonLDValue[V] =
      JsonLDValue[V](value, Some(entityType))
  }

  private[jsonld] final case object JsonLDNull extends JsonLD {
    override lazy val toJson:      Json                            = Json.Null
    override lazy val entityId:    Option[EntityId]                = None
    override lazy val entityTypes: Option[EntityTypes]             = None
    override lazy val flatten:     Either[MalformedJsonLD, JsonLD] = this.asRight
  }

  private[jsonld] final case object JsonLDOptionValue {
    def apply[V](maybeValue: Option[V])(implicit encoder: JsonLDEncoder[V]): JsonLD =
      maybeValue match {
        case None    => JsonLD.JsonLDNull
        case Some(v) => encoder(v)
      }
  }

  private[jsonld] final case class JsonLDArray(jsons: Seq[JsonLD]) extends JsonLD {
    override lazy val toJson:      Json                = Json.arr(jsons.map(_.toJson): _*)
    override lazy val entityId:    Option[EntityId]    = None
    override lazy val entityTypes: Option[EntityTypes] = None
    override lazy val flatten: Either[MalformedJsonLD, JsonLD] =
      jsons
        .foldLeft(Either.right[MalformedJsonLD, List[JsonLD]](List.empty[JsonLD])) {
          case (Right(acc), jsonLDEntity: JsonLDEntity) =>
            val (updatedProperties, entities) = deNest(jsonLDEntity.properties, List.empty[JsonLDEntity])
            NonEmptyList.fromList(updatedProperties) match {
              case Some(properties) => Right(entities ++ acc :+ jsonLDEntity.copy(properties = properties))
              case None             => Left(MalformedJsonLD("Empty properties list"))
            }
          case (Right(acc), JsonLDArray(jsonlds)) =>
            val (arrayElements, entities) = deNestJsonLDArray(jsonlds, List.empty[JsonLDEntity])
            Right(entities ++ acc :+ JsonLDArray(arrayElements))
          case (Right(acc), other: JsonLD) => Right(acc :+ other)
          case (error @ Left(_), _) => error
        }
        .map(list => JsonLD.arr(list: _*))
    // maybe can be similar to the JsonLDEntity#flatten?
  }

  private[jsonld] final case class JsonLDEntityId[V <: EntityId](id: V)(implicit encoder: Encoder[V]) extends JsonLD {
    override lazy val toJson:      Json                            = Json.obj("@id" -> id.asJson)
    override lazy val entityId:    Option[EntityId]                = None
    override lazy val entityTypes: Option[EntityTypes]             = None
    override lazy val flatten:     Either[MalformedJsonLD, JsonLD] = this.asRight
  }

  final case class MalformedJsonLD(message: String) extends RuntimeException(message)

  private def deNest(properties: NonEmptyList[(Property, JsonLD)],
                     deNested:   List[JsonLDEntity]
  ): (List[(Property, JsonLD)], List[JsonLDEntity]) =
    properties.toList.foldLeft(List.empty[(Property, JsonLD)], deNested: List[JsonLDEntity]) {
      case ((props, entities), (property, jsonLDEntity: JsonLDEntity)) =>
        val (prop :: propertiesOfNestedEntity, updatedEntities) = deNest(jsonLDEntity.properties, entities)
        val updatedNestedEntity                                 = jsonLDEntity.copy(properties = NonEmptyList(prop, propertiesOfNestedEntity))
        (props :+ (property -> JsonLDEntityId(jsonLDEntity.id))) -> (updatedEntities :+ updatedNestedEntity)

      case ((props, entities), (property, JsonLDArray(jsonLDs))) =>
        val (arrayElements, deNestedEntities) = deNestJsonLDArray(jsonLDs, entities)
        (props :+ property -> JsonLDArray(arrayElements)) -> deNestedEntities
      case ((props, entities), (property, json)) => (props :+ (property -> json)) -> entities
    }
  private def deNestJsonLDArray(jsonLDs:  Seq[JsonLD],
                                entities: List[JsonLDEntity]
  ): (List[JsonLD], List[JsonLDEntity]) =
    jsonLDs.foldLeft((List.empty[JsonLD], entities)) {
      case ((arrayElements, toplevelEntities), jsonLDEntity: JsonLDEntity) =>
        val (prop :: propertiesOfNestedEntity, listOfEntities) = deNest(jsonLDEntity.properties, toplevelEntities)
        val updatedEntity                                      = jsonLDEntity.copy(properties = NonEmptyList(prop, propertiesOfNestedEntity))
        (arrayElements :+ updatedEntity) -> (listOfEntities :+ updatedEntity)
      case ((arrayElements, toplevelEntities), JsonLDArray(jsonLDs)) =>
        val (nestedArrayElements, deNestedEntities) = deNestJsonLDArray(jsonLDs, toplevelEntities)
        (arrayElements :+ JsonLDArray(nestedArrayElements)) -> deNestedEntities
      case ((arrayElements, toplevelEntities), entity) => (arrayElements :+ entity) -> toplevelEntities
    }

}
