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
import java.time.temporal.ChronoUnit.DAYS

import ch.datascience.config.ServiceUrl
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import org.scalacheck.Gen._
import org.scalacheck.{Arbitrary, Gen}

import scala.language.implicitConversions

object Generators {

  def nonEmptyStrings(maxLength: Int = 10): Gen[String] = {
    require(maxLength > 0)

    for {
      length <- choose(1, maxLength)
      chars  <- listOfN(length, alphaChar)
    } yield chars.mkString("")
  }

  def nonEmptyStringsList(minElements: Int = 1, maxElements: Int = 5): Gen[List[String]] =
    for {
      size  <- choose(minElements, maxElements)
      lines <- Gen.listOfN(size, nonEmptyStrings())
    } yield lines

  def positiveInts(max: Int = 1000): Gen[Int] = choose(1, max)

  def nonNegativeInts(max: Int = 1000): Gen[Int] = choose(0, max)

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

  val validatedUrls: Gen[String Refined Url] = httpUrls
    .map { value =>
      RefType
        .applyRef[String Refined Url](value)
        .getOrElse(throw new IllegalArgumentException("Invalid url value"))
    }

  val shas: Gen[String] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'f').map(_.toString)))
  } yield chars.mkString("")

  val timestamps: Gen[Instant] =
    Gen
      .choose(Instant.EPOCH.toEpochMilli, Instant.now().plus(2000, DAYS).toEpochMilli)
      .map(Instant.ofEpochMilli)

  val timestampsInThePast: Gen[Instant] =
    Gen
      .choose(Instant.EPOCH.toEpochMilli, Instant.now().toEpochMilli)
      .map(Instant.ofEpochMilli)

  implicit val exceptions: Gen[Exception] =
    nonEmptyStrings(20).map(new Exception(_))

  implicit val serviceUrls: Gen[ServiceUrl] =
    httpUrls.map(ServiceUrl.apply)

  object Implicits {

    implicit class GenOps[T](generator: Gen[T]) {

      def generateOne: T = generator.sample getOrElse generateOne

      def generateDifferentThan(value: T): T = {
        val generated = generator.sample.getOrElse(generateDifferentThan(value))
        if (generated == value) generateDifferentThan(value)
        else generated
      }
    }
  }
}
