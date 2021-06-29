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
import io.circe.DecodingFailure
import io.renku.jsonld.Cursor.{ArrayCursor, FlattenedArrayCursor, FlattenedJsonCursor, ListItemCursor}
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntity, JsonLDEntityId, JsonLDValue}
import io.renku.jsonld.JsonLDDecoder.Result

/** A type class that provides a conversion from a [[Cursor]] to an object of type `A`
  */
trait JsonLDDecoder[A] extends Serializable {
  def apply(cursor: Cursor): Result[A]
}

abstract class JsonLDEntityDecoder[A](val entityTypes: EntityTypes) extends JsonLDDecoder[A] {
  def apply(cursor: Cursor): Result[A]
}

object JsonLDDecoder {

  type Result[A] = Either[DecodingFailure, A]

  final def instance[A](f: Cursor => Result[A]): JsonLDDecoder[A] = (c: Cursor) => f(c)

  final def entity[A](entityTypes: EntityTypes)(f: Cursor => Result[A]): JsonLDDecoder[A] =
    new JsonLDEntityDecoder[A](entityTypes) {
      override def apply(cursor: Cursor): Result[A] = cursor match {
        case c: FlattenedJsonCursor =>
          c.findEntity(entityTypes)
            .map(entityJson => goDownType(new FlattenedJsonCursor(c, entityJson, c.allEntities)))
            .getOrElse {
              DecodingFailure(
                s"Cannot find an entity of type(s) ${entityTypes.list.map(_.show).nonEmptyIntercalate("; ")}",
                Nil
              ).asLeft
            }
        case _ => goDownType(cursor)
      }

      private def goDownType(cursor: Cursor) = cursor.downType(entityTypes.list.toList: _*) match {
        case Cursor.Empty =>
          DecodingFailure(
            s"Cannot decode to an entity of type(s) ${entityTypes.list.map(_.show).nonEmptyIntercalate("; ")}",
            Nil
          ).asLeft
        case c => f(c)
      }
    }

  implicit val decodeJsonLD: JsonLDDecoder[JsonLD] = _.jsonLD.asRight[DecodingFailure]

  implicit val decodeString: JsonLDDecoder[String] = _.jsonLD match {
    case JsonLDValue(value: String, _) => Right(value)
    case json => DecodingFailure(s"Cannot decode $json to String", Nil).asLeft
  }

  implicit val decodeEntityId: JsonLDDecoder[EntityId] = _.jsonLD match {
    case JsonLDEntityId(value) => Right(value)
    case json                  => DecodingFailure(s"Cannot decode $json to EntityId", Nil).asLeft
  }

  implicit def decodeList[I](implicit itemDecoder: JsonLDDecoder[I]): JsonLDDecoder[List[I]] = {
    case cursor @ FlattenedArrayCursor(_, array, allEntities) =>
      itemDecoder match {
        case itemDecoder: JsonLDEntityDecoder[I] =>
          for {
            arrayEntityIds <- array.cursor.as[List[EntityId]](decodeList[EntityId])
            entities <-
              arrayEntityIds
                .map(id => Either.fromOption(allEntities.get(id), DecodingFailure(s"No entity found with id $id", Nil)))
                .sequence
            validEntities <- entities
                               .filter(_.cursor.downType(itemDecoder.entityTypes.list.toList: _*) != Cursor.Empty)
                               .map(t => itemDecoder(new FlattenedJsonCursor(cursor, t, allEntities)))
                               .sequence
          } yield validEntities

        case itemDecoder: JsonLDDecoder[I] =>
          cursor.jsonLD.jsons.toList.map(v => itemDecoder(new ListItemCursor(cursor, v))).sequence
      }
    case cursor: ArrayCursor =>
      cursor.jsonLD.jsons.toList.map(v => itemDecoder(new ListItemCursor(cursor, v))).filter(_.isRight).sequence
    case cursor: Cursor =>
      cursor.jsonLD match {
        case JsonLDArray(jsons) =>
          itemDecoder match {
            case itemDecoder: JsonLDEntityDecoder[I] =>
              val allEntitiesMap = jsons.toList.flatMap(json => json.cursor.as[(EntityId, JsonLDEntity)].toList)
              val validEntities =
                allEntitiesMap.filter(_._2.cursor.downType(itemDecoder.entityTypes.list.toList: _*) != Cursor.Empty)
              validEntities.map(t => itemDecoder(new FlattenedJsonCursor(cursor, t._2, allEntitiesMap.toMap))).sequence
            case itemDecoder: JsonLDDecoder[I] =>
              jsons.toList.map(v => itemDecoder(new ListItemCursor(cursor, v))).sequence
          }
        case entity @ JsonLDEntity(_, _, _, _) => entity.cursor.as[I].map(List(_))
        case json                              => DecodingFailure(s"Cannot decode $json to List", Nil).asLeft
      }
  }

  private implicit lazy val decodeJsonLDEntity: JsonLDDecoder[(EntityId, JsonLDEntity)] = _.jsonLD match {
    case entity @ JsonLDEntity(id, _, _, _) => Right(id -> entity)
    case json                               => DecodingFailure(s"Cannot decode $json to JsonLDEntity", Nil).asLeft
  }
}
