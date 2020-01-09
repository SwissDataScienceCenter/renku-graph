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

import cats.data.NonEmptyList
import io.renku.jsonld.JsonLD.JsonLDEntity

abstract class Cursor {

  import Cursor._

  val jsonLD: JsonLD

  def delete: Cursor

  def top: Option[JsonLD]

  def as[T](implicit decoder: JsonLDDecoder[T]): JsonLDDecoder.Result[T] = decoder(this)

  def downType(searchedType: EntityType): Cursor = jsonLD match {
    case JsonLDEntity(_, types, _) if types.list.exists(_ == searchedType) => this
    case _                                                                 => Empty
  }

  def downField(name: Property): Cursor = jsonLD match {
    case JsonLDEntity(_, _, props) =>
      props
        .find(_._1 == name)
        .fold(Empty: Cursor) { case (name, value) => new PropertyCursor(this, name, value) }
    case _ => Empty
  }
}

object Cursor {
  def from(jsonLD: JsonLD): Cursor = new TopCursor(jsonLD)

  private[jsonld] object Empty extends Cursor {
    override lazy val jsonLD: JsonLD         = JsonLD.JsonLDNull
    override lazy val delete: Cursor         = this
    override lazy val top:    Option[JsonLD] = None
  }

  private[jsonld] class TopCursor(override val jsonLD: JsonLD) extends Cursor {
    override lazy val delete: Cursor         = Empty
    override lazy val top:    Option[JsonLD] = Some(jsonLD)
  }

  private[jsonld] class DeletedPropertyCursor(parent: Cursor, property: Property) extends Cursor {
    override lazy val jsonLD: JsonLD = JsonLD.JsonLDNull
    override lazy val delete: Cursor = this
    override lazy val top: Option[JsonLD] = parent.jsonLD match {
      case json @ JsonLDEntity(_, _, properties) =>
        properties.filterNot {
          case (`property`, _) => true
          case _               => false
        } match {
          case Nil            => None
          case first +: other => Some(json.copy(properties = NonEmptyList.of(first, other: _*)))
        }
    }
  }

  private[jsonld] class PropertyCursor(parent: Cursor, property: Property, override val jsonLD: JsonLD) extends Cursor {
    override lazy val delete: Cursor         = new DeletedPropertyCursor(parent, property)
    override lazy val top:    Option[JsonLD] = parent.top
  }
}
