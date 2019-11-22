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

import cats.data.Validated
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.http.rest.SortBy.Direction.Desc
import org.http4s.ParseFailure
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SortBySpec extends WordSpec with ScalaCheckPropertyChecks {

  "from" should {

    "return the valid Sort.By instance for a valid name and direction" in {
      forAll(sorts) { sort =>
        Sort.from(serialize(sort)) shouldBe Right(sort)
      }
    }

    "return Left for a invalid name" in {
      val Left(exception) = Sort.from(s"invalid:$Desc")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"'invalid' is not a valid sort property. Allowed properties: ${Sort.properties.mkString(", ")}"
    }

    "return Left for a invalid direction" in {
      val Left(exception) = Sort.from(s"${Sort.Name}:invalid")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"'invalid' is neither 'asc' nor 'desc'"
    }

    "return Left for a invalid sort" in {
      val sortAsString = s"${Sort.Name}$Desc"

      val Left(exception) = Sort.from(sortAsString)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"'$sortAsString' is not a valid sort"
    }
  }

  "sort" should {

    "decode a valid sort query parameter" in {
      forAll(sorts) { sort =>
        Map("sort" -> Seq(serialize(sort))) match {
          case Sort.sort(actual) => actual shouldBe Some(Validated.validNel(sort))
        }
      }
    }

    "fail to decode an invalid sort query parameter" in {
      Map("sort" -> Seq(s"invalid:$Desc")) match {
        case Sort.sort(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure(Sort.Property.from("invalid").left.get.getMessage, "")
          })
      }
    }

    "return None when no sort query parameter" in {
      Map.empty[String, List[String]] match {
        case Sort.sort(actual) => actual shouldBe None
      }
    }
  }

  object Sort extends SortBy {
    type PropertyType = TestProperty
    sealed trait TestProperty extends Property
    case object Name          extends Property(name = "name") with TestProperty
    case object Email         extends Property(name = "email") with TestProperty

    override val properties: Set[TestProperty] = Set(Name, Email)
  }

  private implicit lazy val sorts: Gen[Sort.By] = sortBys(Sort)

  private def serialize(sort: Sort.By): String = s"${sort.property}:${sort.direction}"
}
