/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tinytypes.constraints

import io.renku.generators.Generators.timestamps
import io.renku.tinytypes.{InstantTinyType, TinyTypeFactory}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant
import java.time.temporal.ChronoUnit.{HOURS, SECONDS}

class BoundedInstantSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "BoundedInstant - with min and max" should {

    object Type
        extends TinyTypeFactory[InstantTinyType](instant => new InstantTinyType { val value = instant })
        with BoundedInstant {
      protected[this] override lazy val instantNow: Instant         = Instant.now()
      val min:                                      Instant         = instantNow.minus(24, HOURS)
      val max:                                      Instant         = instantNow.plus(24, HOURS)
      protected[this] override def maybeMin:        Option[Instant] = Some(min)
      protected[this] override def maybeMax:        Option[Instant] = Some(max)
    }

    "be instantiatable instants from within the defined boundaries" in {
      forAll(
        timestamps(
          min = Instant.now().minus(24, HOURS).plus(2, SECONDS),
          max = Instant.now().plus(24, HOURS).minus(2, SECONDS)
        )
      ) { value =>
        Type.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail if outside the left boundary" in {
      forAll(
        timestamps(
          max = Instant.now().minus(24, HOURS).minus(2, SECONDS)
        )
      ) { value =>
        val Left(exception) = Type.from(value).map(_.value)

        exception            shouldBe an[IllegalArgumentException]
        exception.getMessage shouldBe s"${Type.typeName} has to be >= ${Type.min} and <= ${Type.max}"
      }
    }

    "fail if outside the right boundary" in {
      forAll(
        timestamps(
          min = Instant.now().plus(24, HOURS).plus(2, SECONDS)
        )
      ) { value =>
        val Left(exception) = Type.from(value).map(_.value)

        exception            shouldBe an[IllegalArgumentException]
        exception.getMessage shouldBe s"${Type.typeName} has to be >= ${Type.min} and <= ${Type.max}"
      }
    }
  }

  "BoundedInstant - with min" should {

    object Type
        extends TinyTypeFactory[InstantTinyType](instant => new InstantTinyType { val value = instant })
        with BoundedInstant {
      protected[this] override lazy val instantNow: Instant         = Instant.now()
      val min:                                      Instant         = instantNow.minus(24, HOURS)
      protected[this] override def maybeMin:        Option[Instant] = Some(min)
    }

    "be instantiatable instants from within the defined boundaries" in {
      forAll(
        timestamps(min = Instant.now().minus(24, HOURS).plus(2, SECONDS))
      ) { value =>
        Type.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail if outside the boundary" in {
      forAll(
        timestamps(
          max = Instant.now().minus(24, HOURS).minus(2, SECONDS)
        )
      ) { value =>
        val Left(exception) = Type.from(value).map(_.value)

        exception            shouldBe an[IllegalArgumentException]
        exception.getMessage shouldBe s"${Type.typeName} has to be >= ${Type.min}"
      }
    }
  }

  "BoundedInstant - with max" should {

    object Type
        extends TinyTypeFactory[InstantTinyType](instant => new InstantTinyType { val value = instant })
        with BoundedInstant {
      protected[this] override lazy val instantNow: Instant         = Instant.now()
      val max:                                      Instant         = instantNow.plus(24, HOURS)
      protected[this] override def maybeMax:        Option[Instant] = Some(max)
    }

    "be instantiatable instants from within the defined boundaries" in {
      forAll(
        timestamps(max = Instant.now().plus(24, HOURS).minus(2, SECONDS))
      ) { value =>
        Type.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail if outside the boundary" in {
      forAll(
        timestamps(
          min = Instant.now().plus(24, HOURS).plus(2, SECONDS)
        )
      ) { value =>
        val Left(exception) = Type.from(value).map(_.value)

        exception            shouldBe an[IllegalArgumentException]
        exception.getMessage shouldBe s"${Type.typeName} has to be <= ${Type.max}"
      }
    }
  }
}
