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

package io.renku.jsonld

import cats.syntax.all._
import io.circe.{Encoder, Json, JsonNumber}
import io.renku.jsonld.JsonLD.MalformedJsonLD
import io.renku.jsonld.flatten.{JsonLDArrayFlatten, JsonLDEntityFlatten}

import java.io.Serializable
import java.time.{Instant, LocalDate}

abstract class JsonLD extends Product with Serializable {
  def toJson: Json

  def entityId: Option[EntityId]

  def entityTypes: Option[EntityTypes]

  def cursor: Cursor = Cursor.from(this)

  def asArray: Option[Vector[JsonLD]]

  def flatten: Either[MalformedJsonLD, JsonLD]

}

object JsonLD {

  import io.circe.syntax._

  val Null: JsonLD = JsonLDNull

  def fromString(value: String): JsonLD = JsonLDValue(value)

  def fromInt(value: Int): JsonLD = JsonLDValue(Json.fromInt(value).asNumber.getOrElse(throw new Exception("")))

  def fromLong(value: Long): JsonLD = JsonLDValue(Json.fromLong(value).asNumber.getOrElse(throw new Exception("")))

  def fromNumber(value: JsonNumber): JsonLD = JsonLDValue(value)

  def fromInstant(value: Instant): JsonLD = JsonLDInstantValue.from(value)

  def fromLocalDate(value: LocalDate): JsonLD = JsonLDLocalDateValue.from(value)

  def fromBoolean(value: Boolean): JsonLD = JsonLDValue(value)

  def fromOption[V](value: Option[V])(implicit encoder: JsonLDEncoder[V]): JsonLD = JsonLDOptionValue(value)

  def fromEntityId(id: EntityId): JsonLD = JsonLDEntityId(id)

  def arr(jsons: JsonLD*): JsonLD = JsonLDArray(jsons)

  def entity(
      id:            EntityId,
      types:         EntityTypes,
      firstProperty: (Property, JsonLD),
      other:         (Property, JsonLD)*
  ): JsonLDEntity = entity(id, types, reverse = Reverse.empty, firstProperty, other: _*)

  def entity(
      id:         EntityId,
      types:      EntityTypes,
      properties: Map[Property, JsonLD],
      other:      (Property, JsonLD)*
  ): JsonLDEntity = JsonLDEntity(id, types, properties ++ other.toList, Reverse.empty)

  def entity(
      id:            EntityId,
      types:         EntityTypes,
      reverse:       Reverse,
      firstProperty: (Property, JsonLD),
      other:         (Property, JsonLD)*
  ): JsonLDEntity = JsonLDEntity(id, types, properties = other.toMap + firstProperty, reverse)

  def entity(
      id:         EntityId,
      types:      EntityTypes,
      reverse:    Reverse,
      properties: Map[Property, JsonLD]
  ): JsonLDEntity = JsonLDEntity(id, types, properties, reverse)

  private[jsonld] final case class JsonLDEntity(id:         EntityId,
                                                types:      EntityTypes,
                                                properties: Map[Property, JsonLD],
                                                reverse:    Reverse
  ) extends JsonLD
      with JsonLDEntityFlatten {

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

    override lazy val entityId:    Option[EntityId]       = Some(id)
    override lazy val entityTypes: Option[EntityTypes]    = Some(types)
    override lazy val asArray:     Option[Vector[JsonLD]] = Some(Vector(this))

  }

  private[jsonld] final case class JsonLDValue[V](
      value:          V,
      maybeType:      Option[EntityTypes] = None
  )(implicit encoder: Encoder[V])
      extends JsonLD {
    override lazy val toJson: Json = maybeType match {
      case None    => Json.obj("@value" -> value.asJson)
      case Some(t) => Json.obj("@type" -> t.asJson, "@value" -> value.asJson)
    }

    override lazy val entityId:    Option[EntityId]    = None
    override lazy val entityTypes: Option[EntityTypes] = None

    override lazy val asArray: Option[Vector[JsonLD]] = Some(Vector(this))

    override lazy val flatten: Either[MalformedJsonLD, JsonLD] = this.asRight
  }

  private[jsonld] object JsonLDValue {
    def apply[V](value: V, entityType: EntityTypes)(implicit encoder: Encoder[V]): JsonLDValue[V] =
      JsonLDValue[V](value, Some(entityType))
  }

  private[jsonld] object JsonLDInstantValue {
    val entityTypes = EntityTypes.of(Schema.from("http://www.w3.org/2001/XMLSchema", "#") / "dateTime")

    def from(instant: Instant): JsonLDValue[Instant] = JsonLDValue(instant, entityTypes.some)
  }

  private[jsonld] object JsonLDLocalDateValue {
    val entityTypes = EntityTypes.of(Schema.from("http://schema.org") / "Date")

    def from(localDate: LocalDate): JsonLDValue[LocalDate] = JsonLDValue(localDate, entityTypes.some)
  }

  private[jsonld] final case object JsonLDNull extends JsonLD {
    override lazy val toJson:      Json                            = Json.Null
    override lazy val entityId:    Option[EntityId]                = None
    override lazy val entityTypes: Option[EntityTypes]             = None
    override lazy val asArray:     Option[Vector[JsonLD]]          = None
    override lazy val flatten:     Either[MalformedJsonLD, JsonLD] = this.asRight
  }

  private[jsonld] final case object JsonLDOptionValue {
    def apply[V](maybeValue: Option[V])(implicit encoder: JsonLDEncoder[V]): JsonLD =
      maybeValue match {
        case None    => JsonLD.JsonLDNull
        case Some(v) => encoder(v)
      }
  }

  private[jsonld] final case class JsonLDArray(jsons: Seq[JsonLD]) extends JsonLD with JsonLDArrayFlatten {

    override def hashCode(): Int = jsons.size.hashCode() + jsons.toSet.hashCode()

    override def equals(that: Any): Boolean = that match {
      case JsonLDArray(otherJsons) => (otherJsons.size == jsons.size) && (otherJsons.toSet == jsons.toSet)
      case _                       => false
    }

    override lazy val toJson:      Json                   = Json.arr(jsons.map(_.toJson): _*)
    override lazy val entityId:    Option[EntityId]       = None
    override lazy val entityTypes: Option[EntityTypes]    = None
    override lazy val asArray:     Option[Vector[JsonLD]] = Some(jsons.toVector)
  }

  private[jsonld] final case class JsonLDEntityId[V <: EntityId](id: V)(implicit encoder: Encoder[V]) extends JsonLD {
    override lazy val toJson:      Json                            = Json.obj("@id" -> id.asJson)
    override lazy val entityId:    Option[EntityId]                = None
    override lazy val entityTypes: Option[EntityTypes]             = None
    override lazy val asArray:     Option[Vector[JsonLD]]          = Some(Vector(this))
    override lazy val flatten:     Either[MalformedJsonLD, JsonLD] = this.asRight
  }

  final case class MalformedJsonLD(message: String) extends RuntimeException(message)
}
