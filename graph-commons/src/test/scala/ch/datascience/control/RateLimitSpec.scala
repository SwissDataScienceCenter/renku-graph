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

package ch.datascience.control

import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.control.RateLimitUnit._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class RateLimitSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  private type EitherWithThrowable[T] = Either[Throwable, T]
  private trait Target

  "from" should {

    "instantiate from '<number>/seq' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from[EitherWithThrowable, Target](s"$int/sec") shouldBe Right(RateLimit(int, Second))
      }
    }

    "instantiate from '<number>/min' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from[EitherWithThrowable, Target](s"$int/min") shouldBe Right(RateLimit(int, Minute))
      }
    }

    "instantiate from '<number>/hour' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from[EitherWithThrowable, Target](s"$int/hour") shouldBe Right(RateLimit(int, Hour))
      }
    }

    "instantiate from '<number>/day' value" in {
      forAll(positiveLongs(max = Integer.MAX_VALUE)) { int =>
        RateLimit.from[EitherWithThrowable, Target](s"$int/day") shouldBe Right(RateLimit(int, Day))
      }
    }

    "return failure if 0" in {
      val Left(exception) = RateLimit.from[EitherWithThrowable, Target]("0/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "ch.datascience.control.RateLimit has to be positive"
    }

    "return failure if negative number" in {
      val Left(exception) = RateLimit.from[EitherWithThrowable, Target]("-1/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Invalid value for ch.datascience.control.RateLimit: '-1/sec'"
    }

    "return failure if not a number" in {
      val Left(exception) = RateLimit.from[EitherWithThrowable, Target]("add/sec")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Invalid value for ch.datascience.control.RateLimit: 'add/sec'"
    }

    "return failure if unit not sec/hour/day" in {
      val Left(exception) = RateLimit.from[EitherWithThrowable, Target]("2/month")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Unknown 'month' for ch.datascience.control.RateLimitUnit"
    }
  }

  "fromConfig" should {

    "return RateLimit if there's valid value for the given key" in {
      val rateLimit = rateLimits[Target].generateOne
      val configKey = nonEmptyStrings().generateOne
      val config = ConfigFactory.parseMap(
        Map(configKey -> rateLimit.toString).asJava
      )

      RateLimit.fromConfig[Try, Target](configKey, config) shouldBe Success(rateLimit)
    }

    "fail if there is no valid value for the given key in the config" in {
      val configKey = nonEmptyStrings().generateOne
      val config    = ConfigFactory.empty

      val Failure(exception) = RateLimit.fromConfig[Try, Target](configKey, config)

      exception shouldBe a[ConfigLoadingException]
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
      forAll(rateLimits[Target], positiveInts()) { (rateLimit, divider) =>
        whenever {
          (rateLimit.items.value * (1 day).toMillis / (rateLimit.per.multiplierFor(MILLISECONDS) * divider)).toLong > 0
        } {
          rateLimit / divider shouldBe Right {
            RateLimit(
              Refined.unsafeApply(
                (rateLimit.items.value * (1 day).toMillis /
                  (rateLimit.per.multiplierFor(MILLISECONDS) * divider.value)).toLong
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
