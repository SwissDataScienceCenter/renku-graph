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

package ch.datascience.http.rest

import cats.implicits._
import org.http4s.dsl.impl.OptionalValidatingQueryParamDecoderMatcher
import org.http4s.{ParseFailure, QueryParamDecoder}

trait SortBy {

  import SortBy.Direction

  abstract class Property(val name: String) extends Product with Serializable {
    override lazy val toString: String = name
  }

  object Property {
    def from(propertyName: String): Either[IllegalArgumentException, Property] =
      Either.fromOption(
        properties.find(_.name.equalsIgnoreCase(propertyName)),
        new IllegalArgumentException(
          s"'$propertyName' is not a valid sort property. Allowed properties: ${properties.mkString(", ")}"
        )
      )
  }

  case class By(property: Property, direction: Direction)

  def properties: Set[Property]

  private val `property:direction` = "^(\\w+)\\:(\\w+)$".r
  def from(propertyAndDirection: String): Either[IllegalArgumentException, By] = propertyAndDirection match {
    case `property:direction`(propertyName, directionName) =>
      (Property.from(propertyName) -> Direction.from(directionName)) mapN By.apply
    case other => Left(new IllegalArgumentException(s"'$other' is not a valid sort"))
  }

  private implicit val sortParameterDecoder: QueryParamDecoder[By] =
    value => from(value.value).leftMap(_.getMessage).leftMap(ParseFailure(_, "")).toValidatedNel

  object sort extends OptionalValidatingQueryParamDecoderMatcher[By]("sort") {
    val parameterName: String = "sort"
  }
}

object SortBy {

  sealed abstract class Direction(val name: String) extends Product with Serializable {
    override lazy val toString: String = name
  }

  object Direction {

    final case object Asc extends Direction("asc")
    type Asc = Asc.type

    final case object Desc extends Direction("desc")
    type Desc = Desc.type

    private val all: Set[Direction] = Set(Asc, Desc)

    def from(direction: String): Either[IllegalArgumentException, Direction] =
      Either.fromOption(
        all.find(_.name.equalsIgnoreCase(direction)),
        ifNone = new IllegalArgumentException(s"'$direction' is neither '$Asc' nor '$Desc'")
      )
  }
}
