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

package ch.datascience.control

import cats.implicits._
import ch.datascience.control.RateLimitUnit._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.language.postfixOps

class RateLimitSpec extends WordSpec with ScalaCheckPropertyChecks {

  private type EitherWithThrowable[T] = Either[Throwable, T]

  "apply" should {

    "instantiate from '<number>/seq' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from[EitherWithThrowable](s"$int/sec") shouldBe Right(RateLimit(int, Second))
      }
    }

    "instantiate from '<number>/min' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from[EitherWithThrowable](s"$int/min") shouldBe Right(RateLimit(int, Minute))
      }
    }

    "instantiate from '<number>/hour' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from[EitherWithThrowable](s"$int/hour") shouldBe Right(RateLimit(int, Hour))
      }
    }

    "instantiate from '<number>/day' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from[EitherWithThrowable](s"$int/day") shouldBe Right(RateLimit(int, Day))
      }
    }

    "return failure if 0" in {
      val Left(exception) = RateLimit.from[EitherWithThrowable]("0/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "ch.datascience.control.RateLimit has to be positive"
    }

    "return failure if negative number" in {
      val Left(exception) = RateLimit.from[EitherWithThrowable]("-1/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Invalid value for ch.datascience.control.RateLimit: '-1/sec'"
    }

    "return failure if not a number" in {
      val Left(exception) = RateLimit.from[EitherWithThrowable]("add/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Invalid value for ch.datascience.control.RateLimit: 'add/sec'"
    }

    "return failure if unit not sec/hour/day" in {
      val Left(exception) = RateLimit.from[EitherWithThrowable]("2/month")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Unknown 'month' for ch.datascience.control.RateLimitUnit"
    }
  }

  "toString" should {

    "produce a string in format '<number>/<unit>'" in {
      forAll(rateLimits) {
        case rateLimit @ RateLimit(rate, Second) => rateLimit.toString shouldBe s"$rate/sec"
        case rateLimit @ RateLimit(rate, Minute) => rateLimit.toString shouldBe s"$rate/min"
        case rateLimit @ RateLimit(rate, Hour)   => rateLimit.toString shouldBe s"$rate/hour"
        case rateLimit @ RateLimit(rate, Day)    => rateLimit.toString shouldBe s"$rate/day"
      }
    }
  }

  "/" should {

    "make the rate limit x times slower than initially" in {
      forAll(rateLimits, positiveInts()) { (rateLimit, value) =>
        whenever {
          (rateLimit.items.value * (1 day).toMillis / (rateLimit.per.multiplierFor(MILLISECONDS) * value)).toLong > 0
        } {
          rateLimit / value shouldBe Right {
            RateLimit(
              Refined.unsafeApply(
                (rateLimit.items.value * (1 day).toMillis /
                  (rateLimit.per.multiplierFor(MILLISECONDS) * value.value)).toLong
              ),
              per = Day
            )
          }
        }
      }
    }

    "fail if resulting RateLimit below 1/day" in {
      val Left(exception) = RateLimit(1L, per = Day) / 2
      exception.getMessage shouldBe "RateLimits below 1/day not supported"
    }
  }
}
