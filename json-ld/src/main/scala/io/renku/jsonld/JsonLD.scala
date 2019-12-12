/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.data.NonEmptyList
import io.circe.{Encoder, Json}

abstract class JsonLD extends Product with Serializable {
  def toJson: Json
}

object JsonLD {

  import io.circe.syntax._

  def entity(id: EntityId, entityType: EntityType, first: (Property, JsonLD), subsequent: (Property, JsonLD)*): JsonLD =
    entity(id, NonEmptyList.one(entityType), NonEmptyList.of(first, subsequent: _*))

  def entity(id: EntityId, types: NonEmptyList[EntityType], properties: NonEmptyList[(Property, JsonLD)]): JsonLD =
    JsonLDEntity(id, types, properties)

  def fromString(value: String):   JsonLD = JsonLDValue(value)
  def fromInt(value:    Int):      JsonLD = JsonLDValue(value)
  def fromLong(value:   Long):     JsonLD = JsonLDValue(value)
  def fromEntityId(id:  EntityId): JsonLD = JsonLDEntityId(id)
  def arr(jsons:        JsonLD*):  JsonLD = JsonLDArray(jsons)

  private[jsonld] final case class JsonLDEntity(id:         EntityId,
                                                types:      NonEmptyList[EntityType],
                                                properties: NonEmptyList[(Property, JsonLD)])
      extends JsonLD {

    override lazy val toJson: Json = Json.obj(
      List(
        "@id"   -> id.asJson,
        "@type" -> serialize(types.toList)
      ) ++ properties.toList.map(toObjectProperties): _*
    )

    private lazy val serialize: List[EntityType] => Json = {
      case only +: Nil => only.asJson
      case multiple    => Json.arr(multiple.map(_.asJson): _*)
    }

    private lazy val toObjectProperties: ((Property, JsonLD)) => (String, Json) = {
      case (property, value) => property.url -> value.toJson
    }
  }

  private[jsonld] final case class JsonLDValue[V](value: V)(implicit encoder: Encoder[V]) extends JsonLD {
    override lazy val toJson = Json.obj("@value" -> value.asJson)
  }

  private[jsonld] final case class JsonLDArray(jsons: Seq[JsonLD]) extends JsonLD {
    override lazy val toJson = Json.arr(jsons.map(_.toJson): _*)
  }

  private[jsonld] final case class JsonLDEntityId[V <: EntityId](id: V)(implicit encoder: Encoder[V]) extends JsonLD {
    override lazy val toJson = Json.obj("@id" -> id.asJson)
  }
}
