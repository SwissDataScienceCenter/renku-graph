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

import cats.Show
import io.circe.{Decoder, Encoder, Json}

import java.util.UUID

abstract class EntityId extends Product with Serializable {
  type Value
  def value:  Value
  def asJson: Json

  def valueShow: Show[Value]
}

object EntityId {

  def of[T](value: T)(implicit convert: T => EntityId): EntityId = convert(value)
  def blank:                                            EntityId = BlankNodeEntityId(UUID.randomUUID())

  private[jsonld] final case class StandardEntityId(override val value: String) extends EntityId {
    type Value = String
    override lazy val toString: String = value
    override lazy val asJson:   Json   = Json.fromString(value)

    override def valueShow: Show[String] = Show[String](_ => value)
  }
  private[jsonld] final case class BlankNodeEntityId(override val value: UUID) extends EntityId {
    type Value = UUID
    override lazy val toString: String = s"_:$value"
    override lazy val asJson:   Json   = Json.fromString(s"_:$value")

    override def valueShow: Show[UUID] = Show[UUID](_ => value.toString)
  }

  implicit val entityIdJsonEncoder: Encoder[EntityId] = Encoder.instance(_.asJson)

  implicit val entityIdJsonDecoder: Decoder[EntityId] = Decoder.instance {
    _.as[String].map {
      case s if s.startsWith("_:") => EntityId.blank
      case s                       => EntityId.of(s)
    }
  }

  implicit val stringToEntityId:   String => EntityId   = StandardEntityId.apply
  implicit val propertyToEntityId: Property => EntityId = p => StandardEntityId(p.url)

  implicit val show: Show[EntityId] = Show[EntityId](entityId => entityId.valueShow.show(entityId.value))
}
