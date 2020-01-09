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
import io.circe.{Encoder, Json}

abstract class JsonLD extends Product with Serializable {
  def toJson:   Json
  def entityId: Option[EntityId]
  def cursor: Cursor = Cursor.from(this)
}

object JsonLD {

  import io.circe.syntax._

  val Null: JsonLD = JsonLDNull

  def fromString(value:    String): JsonLD = JsonLDValue(value)
  def fromInt(value:       Int): JsonLD = JsonLDValue(value)
  def fromLong(value:      Long): JsonLD = JsonLDValue(value)
  def fromInstant(value:   Instant): JsonLD = JsonLDValue(value, "http://www.w3.org/2001/XMLSchema#dateTime")
  def fromLocalDate(value: LocalDate): JsonLD = JsonLDValue(value, "http://schema.org/Date")
  def fromOption[V](value: Option[V])(implicit encoder: JsonLDEncoder[V]): JsonLD = JsonLDOptionValue(value)
  def fromEntityId(id:     EntityId): JsonLD = JsonLDEntityId(id)
  def arr(jsons:           JsonLD*): JsonLD = JsonLDArray(jsons)

  def entity(
      id:            EntityId,
      types:         EntityTypes,
      firstProperty: (Property, JsonLD),
      other:         (Property, JsonLD)*
  ): JsonLD = JsonLDEntity(id, types, properties = NonEmptyList.of(firstProperty, other: _*))

  private[jsonld] final case class JsonLDEntity(id:         EntityId,
                                                types:      EntityTypes,
                                                properties: NonEmptyList[(Property, JsonLD)])
      extends JsonLD {

    override lazy val toJson: Json = Json.obj(
      List(
        "@id"   -> id.asJson,
        "@type" -> types.asJson
      ) ++ properties.toList.map(toObjectProperties).flatten: _*
    )

    private lazy val toObjectProperties: ((Property, JsonLD)) => Option[(String, Json)] = {
      case (_, JsonLDNull)   => None
      case (property, value) => Some(property.url -> value.toJson)
    }

    override lazy val entityId: Option[EntityId] = Some(id)
  }

  private[jsonld] final case class JsonLDValue[V](
      value:          V,
      maybeType:      Option[String] = None
  )(implicit encoder: Encoder[V])
      extends JsonLD {
    override lazy val toJson: Json = maybeType match {
      case None    => Json.obj("@value" -> value.asJson)
      case Some(t) => Json.obj("@type"  -> t.asJson, "@value" -> value.asJson)
    }

    override lazy val entityId: Option[EntityId] = None
  }

  private[jsonld] object JsonLDValue {
    def apply[V](value: V, entityType: String)(implicit encoder: Encoder[V]): JsonLDValue[V] =
      JsonLDValue[V](value, Some(entityType))
  }

  private[jsonld] final case object JsonLDNull extends JsonLD {
    override lazy val toJson:   Json             = Json.Null
    override lazy val entityId: Option[EntityId] = None
  }

  private[jsonld] final case object JsonLDOptionValue {
    def apply[V](maybeValue: Option[V])(implicit encoder: JsonLDEncoder[V]): JsonLD =
      maybeValue match {
        case None    => JsonLD.JsonLDNull
        case Some(v) => encoder(v)
      }
  }

  private[jsonld] final case class JsonLDArray(jsons: Seq[JsonLD]) extends JsonLD {
    override lazy val toJson:   Json             = Json.arr(jsons.map(_.toJson): _*)
    override lazy val entityId: Option[EntityId] = None
  }

  private[jsonld] final case class JsonLDEntityId[V <: EntityId](id: V)(implicit encoder: Encoder[V]) extends JsonLD {
    override lazy val toJson:   Json             = Json.obj("@id" -> id.asJson)
    override lazy val entityId: Option[EntityId] = None
  }
}
