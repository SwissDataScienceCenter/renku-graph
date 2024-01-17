/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.http.rest

import cats.Show
import cats.data.ValidatedNel
import cats.syntax.all._
import org.http4s.dsl.impl.OptionalMultiQueryParamDecoderMatcher
import org.http4s.{ParseFailure, Query, QueryParamDecoder}

trait SortBy {

  import SortBy.Direction

  type PropertyType <: Property

  abstract class Property(val name: String) extends Product with Serializable {
    override lazy val toString: String = name
  }

  object Property {
    def from(propertyName: String): Either[IllegalArgumentException, PropertyType] =
      Either.fromOption(
        properties.find(_.name.equalsIgnoreCase(propertyName)),
        new IllegalArgumentException(sort.errorMessage(propertyName))
      )
  }

  case class By(property: PropertyType, direction: Direction)

  def properties: Set[PropertyType]

  private val `property:direction` = "^(\\w+)\\:(\\w+)$".r
  def from(propertyAndDirection: String): Either[IllegalArgumentException, By] = propertyAndDirection match {
    case `property:direction`(propertyName, directionName) =>
      (Property.from(propertyName) -> Direction.from(directionName)) mapN By.apply
    case other => Left(new IllegalArgumentException(s"'$other' is not a valid sort"))
  }

  private implicit val sortParameterDecoder: QueryParamDecoder[By] =
    value => from(value.value).leftMap(_.getMessage).leftMap(ParseFailure(_, "")).toValidatedNel

  object sort extends OptionalMultiQueryParamDecoderMatcher[By]("sort") {
    val parameterName: String = "sort"
    def errorMessage(propertyName: String): String =
      s"'$propertyName' is not a valid $parameterName property. Allowed properties: ${properties.mkString(", ")}"
    def find(query: Query): Option[ValidatedNel[ParseFailure, List[By]]] = sort.unapply(query.multiParams)
  }
}

object SortBy {

  sealed abstract class Direction(val name: String) extends Product with Serializable {
    override lazy val toString: String = name
  }

  object Direction {

    final case object Asc extends Direction("ASC") {
      implicit lazy val ascShow: Show[Asc] = Show.show(asc => show"${asc.name}")
    }
    type Asc = Asc.type

    final case object Desc extends Direction("DESC") {
      implicit lazy val descShow: Show[Desc] = Show.show(desc => show"${desc.name}")
    }
    type Desc = Desc.type

    private val all: Set[Direction] = Set(Asc, Desc)

    def from(direction: String): Either[IllegalArgumentException, Direction] =
      Either.fromOption(
        all.find(_.name.equalsIgnoreCase(direction)),
        ifNone = new IllegalArgumentException(s"'$direction' is neither '$Asc' nor '$Desc'")
      )

    implicit lazy val directionShow: Show[Direction] = Show.show {
      case Asc  => Asc.show
      case Desc => Desc.show
    }
  }
}
