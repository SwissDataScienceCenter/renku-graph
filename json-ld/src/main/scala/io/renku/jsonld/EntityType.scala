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

import io.circe.{Encoder, Json}

abstract class EntityType(val value: String) extends Product with Serializable {
  override lazy val toString: String = value
}

object EntityType {

  def of(url:      String):   EntityType = UrlEntityType(url)
  def of(property: Property): EntityType = UrlEntityType(property.url)

  private[jsonld] final case class UrlEntityType(override val value: String) extends EntityType(value)

  implicit val entityTypeJsonEncoder: Encoder[EntityType] = Encoder.instance(t => Json.fromString(t.value))
}

import cats.data.NonEmptyList

final case class EntityTypes(list: NonEmptyList[EntityType]) {
  lazy val toList: List[EntityType] = list.toList
}

object EntityTypes {

  def of(first: EntityType, other: EntityType*): EntityTypes = EntityTypes(NonEmptyList.of(first, other: _*))
  def of(first: Property, other:   Property*): EntityTypes = EntityTypes {
    NonEmptyList.of(first, other: _*) map EntityType.of
  }

  import io.circe.syntax._

  implicit val entityTypesJsonEncoder: Encoder[EntityTypes] = Encoder.instance { t =>
    t.toList match {
      case only +: Nil => only.asJson
      case multiple    => Json.arr(multiple.map(_.asJson): _*)
    }
  }
}
