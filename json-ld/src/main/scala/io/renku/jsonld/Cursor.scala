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
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLD._
import io.renku.jsonld.syntax._

abstract class Cursor {

  import Cursor._

  val jsonLD: JsonLD

  def delete: Cursor

  def top: Option[JsonLD]

  def as[T](implicit decoder: JsonLDDecoder[T]): JsonLDDecoder.Result[T] = decoder(this) match {
    case result @ Right(_) => result
    case failure           => tryDecodeSingleItemArray(failure)
  }

  private def tryDecodeSingleItemArray[T](failure: JsonLDDecoder.Result[T])(implicit decoder: JsonLDDecoder[T]) =
    jsonLD.asArray
      .map {
        case jsons if jsons.isEmpty => failure
        case head +: tail if tail.isEmpty =>
          this match {
            case cursor @ FlattenedArrayCursor(_, _, allEntities) =>
              decoder(FlattenedJsonCursor(cursor, head, allEntities))
            case _ => decoder(ListItemCursor(this, head))
          }
        case _ => failure
      }
      .getOrElse(failure)

  def getEntityTypes: JsonLDDecoder.Result[EntityTypes] = jsonLD match {
    case JsonLDEntity(_, entityTypes, _, _) => Right(entityTypes)
    case _                                  => Left(DecodingFailure("No EntityTypes found on non-JsonLDEntity object", Nil))
  }

  def downEntityId: Cursor = jsonLD match {
    case JsonLDEntity(entityId, _, _, _) => PropertyCursor(this, Property("@id"), entityId.asJsonLD)
    case _: JsonLDEntityId[_] => this
    case JsonLDArray(Seq(jsonLDEntityId @ JsonLDEntityId(_))) => PropertyCursor(this, Property("@id"), jsonLDEntityId)
    case JsonLDArray(items) if items.size != 1                => Empty(s"Expected @id but got an array of size ${items.size}")
    case jsonLD                                               => Empty(s"Expected @id but got a ${jsonLD.getClass.getSimpleName}")
  }

  def downType(searchedTypes: EntityTypes): Cursor = downType(searchedTypes.toList: _*)

  def downType(searchedTypes: EntityType*): Cursor = jsonLD match {
    case JsonLDEntity(_, types, _, _) if searchedTypes.diff(types.list.toList).isEmpty => this
    case _                                                                             => Empty(s"Cannot find entity with ${searchedTypes.mkString("; ")} @type")
  }

  lazy val downArray: Cursor = jsonLD match {
    case array @ JsonLDArray(_) =>
      this match {
        case cursor: FlattenedArrayCursor => cursor
        case cursor: Cursor               => ArrayCursor(cursor, array)
      }
    case jsonLD => Empty(s"Expected JsonLD Array but got ${jsonLD.getClass.getSimpleName}")
  }

  def downField(property: Property): Cursor = jsonLD match {
    case JsonLDEntity(_, _, props, _) =>
      props
        .find(_._1 == property)
        .fold(Empty(show"Cannot find $property property"): Cursor) {
          case (name, entityId @ JsonLDEntityId(_)) =>
            this match {
              case cursor: FlattenedJsonCursor => FlattenedJsonCursor(cursor, entityId, cursor.allEntities)
              case cursor => PropertyCursor(cursor, name, entityId)
            }
          case (name, value: JsonLDValue[_]) => PropertyCursor(this, name, value)
          case (name, entity: JsonLDEntity) => PropertyCursor(this, name, entity)
          case (_, entities: JsonLDArray) =>
            this match {
              case cursor: FlattenedJsonCursor => FlattenedArrayCursor(cursor, entities, cursor.allEntities)
              case cursor => ArrayCursor(cursor, entities)
            }
          case (_, jsonLD) => Empty(s"$property property points to ${jsonLD.getClass.getSimpleName.replace("$", "")}")
        }
    case array @ JsonLDArray(_) =>
      this match {
        case cursor: FlattenedJsonCursor => FlattenedArrayCursor(cursor, array, cursor.allEntities)
        case cursor => ArrayCursor(cursor, array)
      }
    case jsonLD => Empty(s"Expected JsonLD entity or array but got ${jsonLD.getClass.getSimpleName.replace("$", "")}")
  }
}

object Cursor {
  def from(jsonLD: JsonLD): Cursor = TopCursor(jsonLD)

  private[jsonld] final case class Empty(maybeMessage: Option[String]) extends Cursor {
    override lazy val jsonLD: JsonLD         = JsonLD.JsonLDNull
    override lazy val delete: Cursor         = this
    override lazy val top:    Option[JsonLD] = None
  }

  private[jsonld] object Empty {
    val noMessage: Empty = Empty(None)

    def apply(): Empty = noMessage

    def apply(message: String): Empty = Empty(Some(message))

    implicit val show: Show[Empty] = Show.show[Empty] {
      case Empty(Some(message)) => s"Empty cursor cause by $message"
      case Empty(None)          => s"Empty cursor"
    }
  }

  private[jsonld] final case class TopCursor(jsonLD: JsonLD) extends Cursor {
    override lazy val delete: Cursor         = Empty()
    override lazy val top:    Option[JsonLD] = Some(jsonLD)
  }

  private[jsonld] final case class FlattenedJsonCursor(
      parent:      Cursor,
      jsonLD:      JsonLD,
      allEntities: Map[EntityId, JsonLDEntity]
  ) extends Cursor {
    override lazy val delete: Cursor         = Empty()
    override lazy val top:    Option[JsonLD] = parent.top

    def findEntity(entityTypes: EntityTypes): Option[JsonLDEntity] = jsonLD match {
      case JsonLDEntityId(entityId) =>
        allEntities.get(entityId) >>= {
          case entity if entity.types contains entityTypes => Some(entity)
          case _                                           => None
        }
      case entity @ JsonLDEntity(_, types, _, _) if types.contains(entityTypes) => Some(entity)
      case _                                                                    => None
    }
  }

  private[jsonld] final case class DeletedPropertyCursor(parent: Cursor, property: Property) extends Cursor {
    override lazy val jsonLD: JsonLD = JsonLD.JsonLDNull
    override lazy val delete: Cursor = this
    override lazy val top: Option[JsonLD] = parent.jsonLD match {
      case json @ JsonLDEntity(_, _, properties, _) =>
        Some(json.copy(properties = properties.removed(property)))
    }
  }

  private[jsonld] final case class PropertyCursor(parent: Cursor, property: Property, jsonLD: JsonLD) extends Cursor {
    override lazy val delete: Cursor         = DeletedPropertyCursor(parent, property)
    override lazy val top:    Option[JsonLD] = parent.top
  }

  private[jsonld] final case class ListItemCursor(parent: Cursor, jsonLD: JsonLD) extends Cursor {
    override lazy val top:    Option[JsonLD] = parent.top
    override lazy val delete: Cursor         = Empty()
  }

  private[jsonld] final case class ArrayCursor(parent: Cursor, jsonLD: JsonLDArray) extends Cursor {
    override lazy val top:    Option[JsonLD] = parent.top
    override lazy val delete: Cursor         = Empty()
  }

  private[jsonld] final case class FlattenedArrayCursor(parent:      Cursor,
                                                        jsonLD:      JsonLDArray,
                                                        allEntities: Map[EntityId, JsonLDEntity]
  ) extends Cursor {
    override lazy val top:    Option[JsonLD] = parent.top
    override lazy val delete: Cursor         = Empty()
  }
}
