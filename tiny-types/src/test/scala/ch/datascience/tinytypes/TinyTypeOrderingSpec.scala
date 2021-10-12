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

package ch.datascience.tinytypes

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.TestTinyTypes.{InstantTestType, IntTestType, StringTestType}
import org.scalacheck.Gen.uuid
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.util.UUID

class TinyTypeOrderingSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "compareTo" should {

    "work for TinyTypes which values of type with standard Ordering" in {

      forAll(timestamps, timestamps) { (instant1, instant2) =>
        InstantTestType(instant1) compareTo InstantTestType(instant2) shouldBe (instant1 compareTo instant2)
      }

      forAll(negativeInts(), negativeInts()) { (value1, value2) =>
        IntTestType(value1) compareTo IntTestType(value2) shouldBe (value1 compareTo value2)
      }

      forAll(nonBlankStrings(), nonBlankStrings()) { (value1, value2) =>
        StringTestType(value1.value) compareTo StringTestType(value2.value) shouldBe
          (value1.value compareTo value2.value)
      }
    }

    "work for any TinyType when some custom ordering is needed" in {

      val uuid1 = uuid.generateOne
      val uuid2 = uuid.generateOne

      (UUIDTinyType(uuid1) compareTo UUIDTinyType(uuid2)) shouldBe (uuid1.toString compareTo uuid2.toString)
    }
  }

  "an implicit instance of Ordering[TinyType]" should {

    "be available for TinyTypes which values of type having standard Ordering" in {
      forAll(timestamps, timestamps) { (instant1, instant2) =>
        implicitly[Ordering[InstantTestType]].compare(InstantTestType(instant1),
                                                      InstantTestType(instant2)
        ) shouldBe (instant1 compareTo instant2)
      }

      forAll(negativeInts(), negativeInts()) { (value1, value2) =>
        implicitly[Ordering[IntTestType]].compare(
          IntTestType(value1),
          IntTestType(value2)
        ) shouldBe (value1 compareTo value2)
      }

      forAll(nonBlankStrings(), nonBlankStrings()) { (value1, value2) =>
        implicitly[Ordering[StringTestType]].compare(
          StringTestType(value1.value),
          StringTestType(value2.value)
        ) shouldBe (value1.value compareTo value2.value)
      }
    }

    "be available for TinyTypes requiring custom value Ordering" in {

      val uuid1 = uuid.generateOne
      val uuid2 = uuid.generateOne

      implicitly[Ordering[UUIDTinyType]].compare(
        UUIDTinyType(uuid1),
        UUIDTinyType(uuid2)
      ) shouldBe (uuid1.toString compareTo uuid2.toString)
    }
  }

  private implicit val uuidOrdering: Ordering[UUID] = (x: UUID, y: UUID) => x.toString compareTo y.toString

  private class UUIDTinyType(val value: UUID) extends TinyType {
    type V = UUID
  }

  private object UUIDTinyType extends TinyTypeFactory[UUIDTinyType](new UUIDTinyType(_))
}
