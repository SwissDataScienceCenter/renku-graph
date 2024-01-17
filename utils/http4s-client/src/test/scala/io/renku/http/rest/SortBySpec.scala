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

import cats.data.Validated
import io.renku.http.client.HttpClientGenerators
import io.renku.http.rest.SortBy.Direction._
import org.http4s.ParseFailure
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SortBySpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers with HttpClientGenerators {

  "from" should {

    "return the valid Sort.By instance for a valid name and direction" in {
      forAll(testSortBys) { sort =>
        TestSort.from(serialize(sort.sortBy.head)) shouldBe Right(sort.sortBy.head)
      }
    }

    "return Left for a invalid name" in {
      val Left(exception) = TestSort.from(s"invalid:$Desc")

      exception shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"'invalid' is not a valid sort property. Allowed properties: ${TestSort.properties.mkString(", ")}"
    }

    "return Left for a invalid direction" in {
      val Left(exception) = TestSort.from(s"${TestSort.Name}:invalid")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"'invalid' is neither '$Asc' nor '$Desc'"
    }

    "return Left for a invalid sort" in {
      val sortAsString = s"${TestSort.Name}$Desc"

      val Left(exception) = TestSort.from(sortAsString)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"'$sortAsString' is not a valid sort"
    }
  }

  "sort" should {

    "decode a valid sort query parameter" in {
      forAll(testSortBys) { sort =>
        Map("sort" -> Seq(serialize(sort.sortBy.head))) match {
          case TestSort.sort(actual) => actual shouldBe Validated.validNel(sort.sortBy.toList)
        }
      }
    }

    "fail to decode an invalid sort query parameter" in {
      Map("sort" -> Seq(s"invalid:$Desc")) match {
        case TestSort.sort(actual) =>
          actual shouldBe Validated.invalidNel {
            ParseFailure(TestSort.Property.from("invalid").swap.getOrElse(throw new Exception("ERROR!")).getMessage, "")
          }
      }
    }

    "return None when no sort query parameter" in {
      Map.empty[String, List[String]] match {
        case TestSort.sort(actual) => actual shouldBe Validated.valid(Nil)
      }
    }
  }

  private def serialize(sort: TestSort.By): String = s"${sort.property}:${sort.direction}"
}
