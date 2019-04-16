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

import java.time.temporal.ChronoUnit

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators._
import eu.timepit.refined.api.Refined
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.language.postfixOps

class RateLimitSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "instantiate from '<number>/seq' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from(s"$int/sec") shouldBe Right(RateLimit(int, 1 second))
      }
    }

    "instantiate from '<number>/min' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from(s"$int/min") shouldBe Right(RateLimit(int, 1 minute))
      }
    }

    "instantiate from '<number>/hour' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from(s"$int/hour") shouldBe Right(RateLimit(int, 1 hour))
      }
    }

    "instantiate from '<number>/day' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from(s"$int/day") shouldBe Right(RateLimit(int, 1 day))
      }
    }

    "return failure if 0" in {
      val Left(exception) = RateLimit.from("0/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "ch.datascience.control.RateLimit has to be positive"
    }

    "return failure if negative number" in {
      val Left(exception) = RateLimit.from("-1/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Invalid value for ch.datascience.control.RateLimit: '-1/sec'"
    }

    "return failure if not a number" in {
      val Left(exception) = RateLimit.from("add/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Invalid value for ch.datascience.control.RateLimit: 'add/sec'"
    }

    "return failure if unit not sec/hour/day" in {
      val Left(exception) = RateLimit.from("2/month")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Unknown unit 'month' for ch.datascience.control.RateLimit"
    }
  }

  "toString" should {

    "produce a string in format '<number>/<unit>'" in {
      forAll(rateLimits) {
        case rateLimit @ RateLimit(rate, unit) if unit._2.name() == ChronoUnit.SECONDS.name() =>
          rateLimit.toString shouldBe s"$rate/sec"
        case rateLimit @ RateLimit(rate, unit) if unit._2.name() == ChronoUnit.MINUTES.name() =>
          rateLimit.toString shouldBe s"$rate/min"
        case rateLimit @ RateLimit(rate, unit) if unit._2.name() == ChronoUnit.HOURS.name() =>
          rateLimit.toString shouldBe s"$rate/hour"
        case rateLimit @ RateLimit(rate, unit) if unit._2.name() == ChronoUnit.DAYS.name() =>
          rateLimit.toString shouldBe s"$rate/day"
      }
    }
  }

  "/" should {

    "make the rate limit x times slower than before" in {
      forAll(rateLimits, positiveInts()) { (rateLimit, value) =>
        rateLimit / value shouldBe Right(
          RateLimit(
            Refined.unsafeApply(rateLimit.items.value * (1 day).toMillis / (rateLimit.per.toMillis * value.value)),
            1 day
          )
        )
      }
    }
  }
}
