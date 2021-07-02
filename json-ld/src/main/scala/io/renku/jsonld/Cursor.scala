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

import io.circe.DecodingFailure
import io.renku.jsonld.JsonLD._
import io.renku.jsonld.syntax._

abstract class Cursor {

  import Cursor._

  val jsonLD: JsonLD

  def delete: Cursor

  def top: Option[JsonLD]

  def as[T](implicit decoder: JsonLDDecoder[T]): JsonLDDecoder.Result[T] = decoder(this)

  def getEntityTypes: JsonLDDecoder.Result[EntityTypes] = jsonLD match {
    case JsonLDEntity(_, entityTypes, _, _) => Right(entityTypes)
    case _                                  => Left(DecodingFailure("No EntityTypes found on non-JsonLDEntity object", Nil))
  }

  def downEntityId: Cursor = jsonLD match {
    case JsonLDEntity(entityId, _, _, _) => new PropertyCursor(this, Property("@id"), entityId.asJsonLD)
    case _: JsonLDEntityId[_] => this
    case _ => Empty
  }

  def downType(searchedTypes: EntityTypes): Cursor = downType(searchedTypes.toList: _*)

  def downType(searchedTypes: EntityType*): Cursor = jsonLD match {
    case JsonLDEntity(_, types, _, _) if searchedTypes.diff(types.list.toList).isEmpty => this
    case _                                                                             => Empty
  }

  lazy val downArray: Cursor = jsonLD match {
    case array @ JsonLDArray(_) =>
      this match {
        case cursor: FlattenedArrayCursor => FlattenedArrayCursor(cursor, array, cursor.allEntities)
        case cursor: Cursor               => new ArrayCursor(cursor, array)
      }
    case _ => Empty
  }

  def downField(name: Property): Cursor = jsonLD match {
    case JsonLDEntity(_, _, props, _) =>
      props
        .find(_._1 == name)
        .fold(Empty: Cursor) {
          case (name, entityId @ JsonLDEntityId(_)) =>
            this match {
              case cursor: FlattenedJsonCursor => new FlattenedJsonCursor(cursor, entityId, cursor.allEntities)
              case cursor => new PropertyCursor(cursor, name, entityId)
            }
          case (name, value: JsonLDValue[_]) => new PropertyCursor(this, name, value)
          case (name, entity: JsonLDEntity) => new PropertyCursor(this, name, entity)
          case (_, entities: JsonLDArray) =>
            this match {
              case cursor: FlattenedJsonCursor => FlattenedArrayCursor(cursor, entities, cursor.allEntities)
              case cursor => new ArrayCursor(cursor, entities)
            }
          case _ => throw new UnsupportedOperationException("needs implementation")
        }
    case array @ JsonLDArray(_) =>
      this match {
        case cursor: FlattenedJsonCursor => new FlattenedArrayCursor(cursor, array, cursor.allEntities)
        case cursor => new ArrayCursor(cursor, array)
      }
    case _ => Empty
  }
}

object Cursor {
  def from(jsonLD: JsonLD): Cursor = new TopCursor(jsonLD)

  private[jsonld] type Empty = Empty.type
  private[jsonld] object Empty extends Cursor {
    override lazy val jsonLD: JsonLD         = JsonLD.JsonLDNull
    override lazy val delete: Cursor         = this
    override lazy val top:    Option[JsonLD] = None
  }

  private[jsonld] class TopCursor(override val jsonLD: JsonLD) extends Cursor {
    override lazy val delete: Cursor         = Empty
    override lazy val top:    Option[JsonLD] = Some(jsonLD)
  }

  private[jsonld] class FlattenedJsonCursor(
      parent:              Cursor,
      override val jsonLD: JsonLD,
      val allEntities:     Map[EntityId, JsonLDEntity]
  ) extends Cursor {
    override lazy val delete: Cursor         = Empty
    override lazy val top:    Option[JsonLD] = parent.top

    def findEntity(entityTypes: EntityTypes): Option[JsonLDEntity] = jsonLD match {
      case JsonLDEntityId(entityId) =>
        allEntities.get(entityId).flatMap { entity =>
          if (entity.types.contains(entityTypes)) Some(entity)
          else None
        }
      case entity @ JsonLDEntity(_, types, _, _) if types.contains(entityTypes) => Some(entity)
      case _                                                                    => None
    }
  }

  private[jsonld] class DeletedPropertyCursor(parent: Cursor, property: Property) extends Cursor {
    override lazy val jsonLD: JsonLD = JsonLD.JsonLDNull
    override lazy val delete: Cursor = this
    override lazy val top: Option[JsonLD] = parent.jsonLD match {
      case json @ JsonLDEntity(_, _, properties, _) =>
        Some(json.copy(properties = properties.removed(property)))
    }
  }

  private[jsonld] class PropertyCursor(parent: Cursor, property: Property, override val jsonLD: JsonLD) extends Cursor {
    override lazy val delete: Cursor         = new DeletedPropertyCursor(parent, property)
    override lazy val top:    Option[JsonLD] = parent.top
  }

  private[jsonld] class ListItemCursor(parent: Cursor, override val jsonLD: JsonLD) extends Cursor {
    override lazy val top: Option[JsonLD] = parent.top
    override def delete:   Cursor         = Empty
  }

  private[jsonld] class ArrayCursor(parent: Cursor, override val jsonLD: JsonLDArray) extends Cursor {
    override lazy val top: Option[JsonLD] = parent.top
    override def delete:   Cursor         = Empty
  }

  private[jsonld] case class FlattenedArrayCursor(parent:              Cursor,
                                                  override val jsonLD: JsonLDArray,
                                                  allEntities:         Map[EntityId, JsonLDEntity]
  ) extends Cursor {
    override lazy val top: Option[JsonLD] = parent.top
    override def delete:   Cursor         = Empty
  }
}
