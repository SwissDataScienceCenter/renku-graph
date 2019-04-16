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

import java.util.concurrent.TimeUnit

import cats.implicits._
import ch.datascience.tinytypes.TypeName
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.numeric.Positive
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

case class RateLimit(items: Long Refined Positive, per: FiniteDuration) {

  assert(per._1 == 1L, s"RateLimit's per has to have value 1")
  assert(
    Set(TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS) contains per.unit,
    s"RateLimit does not support ${per.unit.name()}"
  )

  override lazy val toString: String = per.unit match {
    case TimeUnit.SECONDS => s"$items/sec"
    case TimeUnit.MINUTES => s"$items/min"
    case TimeUnit.HOURS   => s"$items/hour"
    case TimeUnit.DAYS    => s"$items/day"
    case other            => throw new IllegalStateException(s"Cannot generate toString for RateLimit with from = $other")
  }
}

object RateLimit extends TypeName {

  private val RateExtractor = """(\d+)[ ]*/(\w+)""".r

  def from(value: String): Either[IllegalArgumentException, RateLimit] = value match {
    case RateExtractor(rate, unit) => (toPositiveLong(rate), toDuration(unit)) mapN (RateLimit(_, _))
    case other                     => Left(new IllegalArgumentException(s"Invalid value for $typeName: '$other'"))
  }

  private val toPositiveLong: String => Either[IllegalArgumentException, Long Refined Positive] = rate =>
    for {
      long <- Try(rate.toLong).toEither
               .leftMap(_ => new IllegalArgumentException(s"$typeName has to be positive"))
      positiveLong <- RefType
                       .applyRef[Long Refined Positive](long)
                       .leftMap(_ => new IllegalArgumentException(s"$typeName has to be positive"))
    } yield positiveLong

  private val toDuration: String => Either[IllegalArgumentException, FiniteDuration] = {
    case "sec"  => Right(1 second)
    case "min"  => Right(1 minute)
    case "hour" => Right(1 hour)
    case "day"  => Right(1 day)
    case other  => Left(new IllegalArgumentException(s"Unknown unit '$other' for $typeName"))
  }

  implicit val rateLimitReader: ConfigReader[RateLimit] =
    ConfigReader.fromString[RateLimit] { value =>
      RateLimit
        .from(value)
        .leftMap(exception => CannotConvert(value, RateLimit.getClass.toString, exception.getMessage))
    }

  implicit class RateLimitOps(rateLimit: RateLimit) {

    def /(divider: Int Refined Positive): Either[IllegalArgumentException, RateLimit] =
      RefType
        .applyRef[Long Refined Positive](
          rateLimit.items.value * (1 day).toMillis / (rateLimit.per.toMillis * divider.value)
        )
        .leftMap(_ => new IllegalArgumentException("RateLimit cannot be initialized with values < 1"))
        .map(RateLimit(_, 1 day))
  }
}
