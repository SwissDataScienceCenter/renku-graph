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
import io.circe.{DecodingFailure, JsonNumber}
import io.renku.jsonld.Cursor.{FlattenedArrayCursor, _}
import io.renku.jsonld.JsonLD._
import io.renku.jsonld.JsonLDDecoder.Result

import java.time.{Instant, LocalDate, OffsetDateTime}

/** A type class that provides a conversion from a [[Cursor]] to an object of type `A`
  */
trait JsonLDDecoder[A] extends (Cursor => Result[A]) with Serializable {

  def apply(cursor: Cursor): Result[A]

  def emap[B](f: A => Either[String, B]): JsonLDDecoder[B] =
    this(_).flatMap(f(_).leftMap(DecodingFailure(_, Nil)))
}

object JsonLDDecoder {

  type Result[A] = Either[DecodingFailure, A]

  final def apply[A](implicit jsonLDDecoder: JsonLDDecoder[A]): JsonLDDecoder[A] = jsonLDDecoder

  final def instance[A](f: Cursor => Result[A]): JsonLDDecoder[A] = (c: Cursor) => f(c)

  final def entity[A](
      entityTypes: EntityTypes,
      predicate:   Cursor => JsonLDDecoder.Result[Boolean] = _ => Right(true)
  )(f:             Cursor => Result[A]): JsonLDEntityDecoder[A] = new JsonLDEntityDecoder[A](entityTypes, predicate)(f)

  implicit val decodeJsonLD: JsonLDDecoder[JsonLD] = _.jsonLD.asRight[DecodingFailure]

  implicit val decodeString: JsonLDDecoder[String] = _.jsonLD match {
    case JsonLDValue(value: String, _) => Right(value)
    case JsonLDValue(value, _) => DecodingFailure(s"Cannot decode $value to String", Nil).asLeft
    case json                  => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to String", Nil).asLeft
  }

  implicit val decodeLong: JsonLDDecoder[Long] = _.jsonLD match {
    case JsonLDValue(value: JsonNumber, _) =>
      value.toLong.map(_.asRight).getOrElse(DecodingFailure(s"Cannot decode $value to Long", Nil).asLeft)
    case JsonLDValue(value, _) => DecodingFailure(s"Cannot decode $value to Long", Nil).asLeft
    case json                  => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to Long", Nil).asLeft
  }

  implicit val decodeInt: JsonLDDecoder[Int] = _.jsonLD match {
    case JsonLDValue(value: JsonNumber, _) =>
      value.toInt.map(_.asRight).getOrElse(DecodingFailure(s"Cannot decode $value to Int", Nil).asLeft)
    case JsonLDValue(value, _) => DecodingFailure(s"Cannot decode $value to Int", Nil).asLeft
    case json                  => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to Int", Nil).asLeft
  }

  implicit val decodeBoolean: JsonLDDecoder[Boolean] = _.jsonLD match {
    case JsonLDValue(value: Boolean, _) => Right(value)
    case JsonLDValue(value, _) => DecodingFailure(s"Cannot decode $value to Boolean", Nil).asLeft
    case json                  => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to Boolean", Nil).asLeft
  }

  implicit val decodeInstant: JsonLDDecoder[Instant] = _.jsonLD match {
    case JsonLDValue(value: Instant, Some(JsonLDInstantValue.entityTypes)) => Right(value)
    case JsonLDValue(value: String, _) =>
      (Either.catchNonFatal(Instant.parse(value)) orElse Either.catchNonFatal(OffsetDateTime.parse(value).toInstant))
        .leftMap(e => DecodingFailure(s"Cannot decode $value to Instant: ${e.getMessage}", Nil))
    case JsonLDValue(value, _) => DecodingFailure(s"Cannot decode $value to Instant", Nil).asLeft
    case json                  => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to Instant", Nil).asLeft
  }

  implicit val decodeLocalDate: JsonLDDecoder[LocalDate] = _.jsonLD match {
    case JsonLDValue(value: LocalDate, Some(JsonLDLocalDateValue.entityTypes)) => Right(value)
    case JsonLDValue(value: String, _) =>
      Either
        .catchNonFatal(LocalDate.parse(value))
        .leftMap(e => DecodingFailure(s"Cannot decode $value to LocalDate: ${e.getMessage}", Nil))
    case JsonLDValue(value, _) => DecodingFailure(s"Cannot decode $value to LocalDate", Nil).asLeft
    case json                  => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to LocalDate", Nil).asLeft
  }

  implicit val decodeEntityId: JsonLDDecoder[EntityId] = _.jsonLD match {
    case JsonLDEntityId(value) => Right(value)
    case json                  => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to EntityId", Nil).asLeft
  }

  implicit val decodeEntityTypes: JsonLDDecoder[EntityTypes] = _.jsonLD match {
    case JsonLDEntity(_, entityTypes, _, _) => Right(entityTypes)
    case json                               => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to EntityTypes", Nil).asLeft
  }

  implicit def decodeOption[I](implicit valueDecoder: JsonLDDecoder[I]): JsonLDDecoder[Option[I]] = { cursor =>
    cursor.jsonLD match {
      case JsonLD.JsonLDNull       => None.asRight[DecodingFailure]
      case JsonLD.JsonLDArray(Nil) => None.asRight[DecodingFailure]
      case JsonLD.JsonLDArray(head :: Nil) =>
        cursor match {
          case cursor @ FlattenedArrayCursor(_, _, allEntities) =>
            valueDecoder(FlattenedJsonCursor(cursor, head, allEntities)) map Option.apply
          case _ => valueDecoder(ListItemCursor(cursor, head)) map Option.apply
        }
      case _ => valueDecoder(cursor) map Option.apply
    }
  }

  implicit def decodeList[I](implicit itemDecoder: JsonLDDecoder[I]): JsonLDDecoder[List[I]] = new JsonLDListDecoder[I]
}

class JsonLDEntityDecoder[A](
    val entityTypes: EntityTypes,
    val predicate:   Cursor => JsonLDDecoder.Result[Boolean]
)(f:                 Cursor => Result[A])
    extends JsonLDDecoder[A] { self =>

  override def apply(cursor: Cursor): Result[A] = cursor match {
    case cur: FlattenedJsonCursor => tryDecode(cur) getOrElse cannotDecodeToEntityTypes(cur)
    case _ => goDownType(cursor)
  }

  private lazy val cannotDecodeToEntityTypes: Cursor => Result[A] = _.jsonLD match {
    case entity: JsonLDEntity =>
      DecodingFailure(
        show"Cannot decode entity with ${entity.id} to entity of type(s) $entityTypes; check entity decoder predicate",
        Nil
      ).asLeft
    case _ => DecodingFailure(show"Cannot find an entity of type(s) $entityTypes", Nil).asLeft
  }

  lazy val allowedEntityTypes: Set[EntityTypes] = Set(entityTypes)

  def widen[B >: A]: JsonLDEntityDecoder[B] = this.asInstanceOf[JsonLDEntityDecoder[B]]

  def orElse[B >: A](
      alternative: JsonLDEntityDecoder[B]
  ): JsonLDEntityDecoder[B] = new JsonLDEntityDecoder[B](entityTypes, predicate)(f) {

    override def apply(cursor: Cursor): Result[B] = cursor match {
      case flattenedCursor: FlattenedJsonCursor =>
        (self.tryDecode(flattenedCursor) orElse alternative.tryDecode(flattenedCursor))
          .getOrElse(noEntityWithTypesFailure)
      case _ => goDownType(cursor)
    }

    private lazy val noEntityWithTypesFailure = DecodingFailure(
      show"Cannot find neither an entity of type(s) ${self.entityTypes} nor ${alternative.entityTypes}",
      Nil
    ).asLeft

    override lazy val allowedEntityTypes: Set[EntityTypes] =
      self.allowedEntityTypes ++ alternative.allowedEntityTypes

    protected override def goDownType(cursor: Cursor) =
      self.goDownType(cursor) orElse alternative.goDownType(cursor)
  }

  protected def tryDecode(flattenedCursor: FlattenedJsonCursor): Option[Result[A]] =
    flattenedCursor
      .findEntity(entityTypes, predicate)
      .map(
        _.flatMap(entityJson =>
          goDownType(FlattenedJsonCursor(flattenedCursor, entityJson, flattenedCursor.allEntities))
        )
      )

  protected def goDownType(cursor: Cursor): Result[A] = cursor.downType(entityTypes) match {
    case cursor @ Cursor.Empty(_) =>
      DecodingFailure(show"Cannot decode to an entity of type(s) $entityTypes $cursor", Nil).asLeft
    case c => f(c)
  }
}

private[jsonld] class JsonLDListDecoder[I](implicit itemDecoder: JsonLDDecoder[I]) extends JsonLDDecoder[List[I]] {
  import JsonLDDecoder._

  override def apply(cursor: Cursor): Result[List[I]] = cursor match {
    case cursor: FlattenedArrayCursor => decodeInCaseOfFlattenedCursor(cursor)
    case cursor: ArrayCursor =>
      cursor.jsonLD.jsons.toList.map(v => itemDecoder(ListItemCursor(cursor, v))).filter(_.isRight).sequence
    case cursor: FlattenedJsonCursor => decodeInCaseOfFlattenedJsonCursor(cursor)
    case cursor: Cursor              => decodeForOtherCursor(cursor)
  }

  private lazy val decodeInCaseOfFlattenedCursor: FlattenedArrayCursor => Result[List[I]] = {
    case cursor @ FlattenedArrayCursor(_, array, allEntities) =>
      itemDecoder match {
        case itemDecoder: JsonLDEntityDecoder[I] =>
          for {
            arrayEntityIds <- array.cursor.as[List[EntityId]](decodeList[EntityId])
            entities <-
              arrayEntityIds
                .map(id => Either.fromOption(allEntities.get(id), DecodingFailure(s"No entity found with id $id", Nil)))
                .sequence
            validEntities <-
              entities
                .filter(entity =>
                  itemDecoder.allowedEntityTypes.exists(allowed => entity.entityTypes.exists(_ contains allowed))
                )
                .map(t => itemDecoder(FlattenedJsonCursor(cursor, t, allEntities)))
                .sequence
          } yield validEntities
        case itemDecoder: JsonLDDecoder[I] =>
          cursor.jsonLD.jsons.toList.map(v => itemDecoder(ListItemCursor(cursor, v))).sequence
      }
  }

  private def decodeInCaseOfFlattenedJsonCursor(cursor: FlattenedJsonCursor): Result[List[I]] = cursor.jsonLD match {
    case entity: JsonLDEntity => entity.cursor.as[I].map(List(_))
    case JsonLDEntityId(entityId) =>
      cursor.allEntities
        .get(entityId)
        .map(_.cursor.as[I].map(List(_)))
        .getOrElse(DecodingFailure(show"Cannot find entity with $entityId", Nil).asLeft)
    case jsonLD => DecodingFailure(s"Cannot decode ${ShowTypeName(jsonLD)} to List", Nil).asLeft
  }

  private def decodeForOtherCursor(cursor: Cursor): Result[List[I]] = cursor.jsonLD match {
    case JsonLDArray(jsons) =>
      itemDecoder match {
        case itemDecoder: JsonLDEntityDecoder[I] =>
          val allEntitiesMap = jsons.toList.flatMap(json => json.cursor.as[(EntityId, JsonLDEntity)].toList)
          val entitiesWithMatchingTypes = allEntitiesMap
            .filter { case (_, entity) =>
              itemDecoder.allowedEntityTypes.exists(allowed => entity.entityTypes.exists(_.contains(allowed)))
            }
          val entitiesMatchingPredicate = entitiesWithMatchingTypes
            .map { case (_, jsonLDEntity) => itemDecoder.predicate(jsonLDEntity.cursor).map(jsonLDEntity -> _) }
            .sequence
            .map(_.collect { case (entity, true) => entity })

          entitiesMatchingPredicate >>= { entities =>
            entities.map(entity => itemDecoder(FlattenedJsonCursor(cursor, entity, allEntitiesMap.toMap))).sequence
          }
        case itemDecoder: JsonLDDecoder[I] =>
          jsons.toList.map(json => itemDecoder(ListItemCursor(cursor, json))).sequence
      }
    case entity @ JsonLDEntity(_, _, _, _) => entity.cursor.as[I].map(List(_))
    case value @ JsonLDValue(_, _)         => value.cursor.as[I].map(List(_))
    case JsonLDNull                        => List.empty[I].asRight
    case jsonLD                            => DecodingFailure(s"Cannot decode ${ShowTypeName(jsonLD)} to List", Nil).asLeft
  }

  private implicit lazy val decodeJsonLDEntity: JsonLDDecoder[(EntityId, JsonLDEntity)] = _.jsonLD match {
    case entity @ JsonLDEntity(id, _, _, _) => Right(id -> entity)
    case json                               => DecodingFailure(s"Cannot decode ${ShowTypeName(json)} to JsonLDEntity", Nil).asLeft
  }
}

private object ShowTypeName {
  def apply(json: JsonLD) = json.getClass.getSimpleName.replace("$", "")
}
