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

import cats.implicits._
import io.circe.{Encoder, Json}
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntity}

final class Reverse private (val properties: List[(Property, JsonLD)]) {

  override def equals(obj: Any): Boolean = obj match {
    case Reverse(otherProperties) => properties == otherProperties
    case _                        => false
  }

  override lazy val toString: String = s"Reverse($properties)"
}

object Reverse {

  def unapply(reverse: Reverse): Option[List[(Property, JsonLD)]] = Some(reverse.properties)

  lazy val empty: Reverse = new Reverse(Nil)

  def ofEntities(first: (Property, JsonLDEntity), other: (Property, JsonLDEntity)*): Reverse =
    new Reverse((first +: other).toList)

  def of(first: (Property, JsonLD), other: (Property, JsonLD)*): Either[Exception, Reverse] =
    fromList((first +: other).toList)

  def of(property: (Property, List[JsonLD])): Either[Exception, Reverse] = {
    val (name, list) = property

    list match {
      case Nil => Reverse.empty.asRight[Exception]
      case nonEmpty =>
        nonEmpty find nonEntity match {
          case None => new Reverse(List(name -> JsonLD.arr(list: _*))).asRight[Exception]
          case _ =>
            new IllegalArgumentException(
              s""""@reverse" "$name" property can exist on entity only"""
            ).asLeft[Reverse]
        }
    }
  }

  def fromList(properties: List[(Property, JsonLD)]): Either[Exception, Reverse] =
    properties match {
      case Nil => Reverse.empty.asRight[Exception]
      case list =>
        Either
          .fromOption(
            list collectFirst {
              case `value which is neither Entity nor Array(Entity)`(exception) => exception
            },
            ifNone = new Reverse(properties)
          )
          .swap
    }

  private object `value which is neither Entity nor Array(Entity)` {
    def unapply(tuple: (Property, JsonLD)): Option[Exception] = tuple match {
      case (_, _: JsonLDEntity) => None
      case (property, JsonLDArray(jsons)) =>
        jsons find nonEntity match {
          case None => None
          case _ =>
            new IllegalArgumentException(
              s""""@reverse" "$property" property has to exist on each object of an array"""
            ).some
        }
      case (property, _) =>
        new IllegalArgumentException(
          s""""@reverse" "$property" property has to exist on an object"""
        ).some
    }
  }

  private val nonEntity: JsonLD => Boolean = {
    case _: JsonLDEntity => false
    case _ => true
  }

  def fromListUnsafe(properties: List[(Property, JsonLD)]): Reverse =
    fromList(properties).fold(throw _, identity)

  implicit val jsonEncoder: Encoder[Reverse] = Encoder.instance {
    case Reverse(Nil)                  => Json.Null
    case Reverse((prop, value) +: Nil) => Json.obj(prop.url -> value.toJson)
    case Reverse(props)                => Json.obj(props.map { case (prop, value) => prop.url -> value.toJson }: _*)
  }
}
