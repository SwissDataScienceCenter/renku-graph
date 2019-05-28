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

package ch.datascience.generators

import java.time.Instant
import java.time.temporal.ChronoUnit.{DAYS, MINUTES => MINS}

import cats.data.NonEmptyList
import ch.datascience.config.ServiceUrl
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string.Url
import io.circe.{Encoder, Json}
import org.scalacheck.Gen._
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

object Generators {

  def nonEmptyStrings(maxLength: Int = 10): Gen[String] = {
    require(maxLength > 0)

    for {
      length <- choose(1, maxLength)
      chars  <- listOfN(length, alphaChar)
    } yield chars.mkString("")
  }

  def nonEmptyStringsList(minElements: Int Refined Positive = 1,
                          maxElements: Int Refined Positive = 5): Gen[List[String]] =
    for {
      size  <- choose(minElements.value, maxElements.value)
      lines <- Gen.listOfN(size, nonEmptyStrings())
    } yield lines

  def nonEmptyList[T](generator:   Gen[T],
                      minElements: Int Refined Positive = 1,
                      maxElements: Int Refined Positive = 5): Gen[NonEmptyList[T]] =
    for {
      size <- choose(minElements.value, maxElements.value)
      list <- Gen.listOfN(size, generator)
    } yield NonEmptyList.fromListUnsafe(list)

  def listOf[T](generator: Gen[T], maxElements: Int Refined Positive = 5): Gen[List[T]] =
    for {
      size <- choose(0, maxElements.value)
      list <- Gen.listOfN(size, generator)
    } yield list

  def positiveInts(max: Int = 1000): Gen[Int Refined Positive] =
    choose(1, max) map Refined.unsafeApply

  def positiveLongs(max: Long = 1000): Gen[Long Refined Positive] =
    choose(1L, max) map Refined.unsafeApply

  def nonNegativeInts(max: Int = 1000): Gen[Int] = choose(0, max)

  def negativeInts(min: Int = -1000): Gen[Int] = choose(min, 0)

  def durations(max: FiniteDuration = 5 seconds): Gen[FiniteDuration] =
    choose(1, max.toMillis)
      .map(FiniteDuration(_, MILLISECONDS))

  def relativePaths(minSegments: Int = 1, maxSegments: Int = 10): Gen[String] =
    for {
      partsNumber <- Gen.choose(minSegments, maxSegments)
      parts       <- Gen.listOfN(partsNumber, nonEmptyStrings())
    } yield parts.mkString("/")

  val httpUrls: Gen[String] = for {
    protocol <- Arbitrary.arbBool.arbitrary map {
                 case true  => "http"
                 case false => "https"
               }
    port <- positiveInts(max = 9999)
    host <- nonEmptyStrings()
  } yield s"$protocol://$host:$port"

  val validatedUrls: Gen[String Refined Url] = httpUrls map Refined.unsafeApply

  val shas: Gen[String] = for {
    length <- Gen.choose(40, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'f').map(_.toString)))
  } yield chars.mkString("")

  val timestamps: Gen[Instant] =
    Gen
      .choose(Instant.EPOCH.toEpochMilli, Instant.now().plus(2000, DAYS).toEpochMilli)
      .map(Instant.ofEpochMilli)

  val timestampsNotInTheFuture: Gen[Instant] =
    Gen
      .choose(Instant.EPOCH.toEpochMilli, Instant.now().toEpochMilli)
      .map(Instant.ofEpochMilli)

  val timestampsInTheFuture: Gen[Instant] =
    Gen
      .choose(Instant.now().plus(10, MINS).toEpochMilli, Instant.now().plus(2000, DAYS).toEpochMilli)
      .map(Instant.ofEpochMilli)

  implicit val serviceUrls:  Gen[ServiceUrl]  = httpUrls.map(ServiceUrl.apply)
  implicit val elapsedTimes: Gen[ElapsedTime] = Gen.choose(0L, 10000L) map ElapsedTime.apply
  implicit val exceptions:   Gen[Exception]   = nonEmptyStrings(20).map(new Exception(_))
  implicit val nestedExceptions: Gen[Exception] = for {
    nestLevels <- positiveInts(5)
    rootCause  <- exceptions
  } yield {
    import Implicits._
    (1 to nestLevels).foldLeft(rootCause) { (nestedException, _) =>
      new Exception(nonEmptyStrings().generateOne, nestedException)
    }
  }

  implicit val jsons: Gen[Json] = {
    import io.circe.syntax._

    val tuples = for {
      key <- nonEmptyStrings(maxLength = 5)
      value <- Gen.oneOf(nonEmptyStrings(maxLength = 5),
                         Arbitrary.arbNumber.arbitrary,
                         Arbitrary.arbBool.arbitrary,
                         Gen.nonEmptyListOf(nonEmptyStrings()))
    } yield key -> value

    val objects = for {
      propertiesNumber <- positiveInts(5)
      tuples           <- Gen.listOfN(propertiesNumber, tuples)
    } yield Map(tuples: _*)

    implicit val mapEncoder: Encoder[Map[String, Any]] = Encoder.instance[Map[String, Any]] { map =>
      Json.obj(
        map.map {
          case (key, value: String)  => key -> Json.fromString(value)
          case (key, value: Number)  => key -> Json.fromBigDecimal(value.doubleValue())
          case (key, value: Boolean) => key -> Json.fromBoolean(value)
          case (key, value: List[_]) => key -> Json.arr(value.map(_.toString).map(Json.fromString): _*)
          case (_, value) =>
            throw new NotImplementedError(
              s"Add support for values of type '${value.getClass}' in the jsons generator"
            )
        }.toSeq: _*
      )
    }

    objects.map(_.asJson)
  }

  object Implicits {

    implicit class GenOps[T](generator: Gen[T]) {

      def generateOne: T = generator.sample getOrElse generateOne

      def generateDifferentThan(value: T): T = {
        val generated = generator.sample.getOrElse(generateDifferentThan(value))
        if (generated == value) generateDifferentThan(value)
        else generated
      }

    }

    implicit def asArbitrary[T](implicit generator: Gen[T]): Arbitrary[T] = Arbitrary(generator)
  }
}
