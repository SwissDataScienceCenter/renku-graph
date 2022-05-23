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

package io.renku.generators

import cats.data.NonEmptyList
import cats.{Applicative, Functor, Semigroupal}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.{Negative, NonNegative, NonPositive, Positive}
import eu.timepit.refined.string.Url
import io.circe.{Encoder, Json}
import org.scalacheck.Gen._
import org.scalacheck.{Arbitrary, Gen}

import java.time.Instant.now
import java.time.temporal.ChronoUnit.{DAYS => JAVA_DAYS, MINUTES => JAVA_MINS}
import java.time.{Duration => JavaDuration, _}
import scala.concurrent.duration._

object Generators {

  type NonBlank = String Refined NonEmpty

  def fixed[V](value: V): Gen[V] = Gen.const(value)

  def emptyOptionOf[T]: Gen[Option[T]] = Gen.const(Option.empty[T])

  def noDashUuid: Gen[String] = uuid.map(_.toString.replace("-", ""))

  def nonEmptyStrings(maxLength: Int = 10, charsGenerator: Gen[Char] = alphaChar): Gen[String] = {
    require(maxLength > 0)
    nonBlankStrings(maxLength = Refined.unsafeApply(maxLength), charsGenerator = charsGenerator) map (_.value)
  }

  def nonBlankStrings(minLength:      Int Refined Positive = 1,
                      maxLength:      Int Refined Positive = 10,
                      charsGenerator: Gen[Char] = alphaChar
  ): Gen[NonBlank] = {
    require(minLength.value <= maxLength.value)

    val lengths =
      if (maxLength.value == 1) const(maxLength.value)
      else if (minLength.value == maxLength.value) const(maxLength.value)
      else frequency(1 -> choose(minLength.value, maxLength.value), 9 -> choose(minLength.value + 1, maxLength.value))

    for {
      length <- lengths
      chars  <- listOfN(length, charsGenerator)
    } yield Refined.unsafeApply(chars.mkString(""))
  }

  def stringsOfLength(length: Int Refined Positive = 10, charsGenerator: Gen[Char] = alphaChar): Gen[String] =
    listOfN(length, charsGenerator).map(_.mkString(""))

  def paragraphs(minElements: Int Refined Positive = 5, maxElements: Int Refined Positive = 10): Gen[NonBlank] =
    nonEmptyStringsList(minElements, maxElements) map (_.mkString(" ")) map Refined.unsafeApply

  def sentences(minWords: Int Refined Positive = 1, maxWords: Int Refined Positive = 10): Gen[NonBlank] =
    nonEmptyStringsList(minWords, maxWords) map (_.mkString(" ")) map Refined.unsafeApply

  def sentenceContaining(phrase: NonBlank): Gen[String] = for {
    prefix <- nonEmptyStrings()
    suffix <- nonEmptyStrings()
  } yield s"$prefix $phrase $suffix"

  def blankStrings(maxLength: Int Refined NonNegative = 10): Gen[String] =
    for {
      length <- choose(0, maxLength.value)
      chars  <- listOfN(length, const(" "))
    } yield chars.mkString("")

  def nonEmptyStringsList(minElements: Int Refined Positive = 1,
                          maxElements: Int Refined Positive = 5
  ): Gen[List[String]] =
    for {
      size  <- choose(minElements.value, maxElements.value)
      lines <- Gen.listOfN(size, nonEmptyStrings())
    } yield lines

  def nonEmptyList[T](generator:   Gen[T],
                      minElements: Int Refined Positive = 1,
                      maxElements: Int Refined Positive = 5
  ): Gen[NonEmptyList[T]] =
    for {
      size <- choose(minElements.value, maxElements.value)
      list <- Gen.listOfN(size, generator)
    } yield NonEmptyList.fromListUnsafe(list)

  def nonEmptySet[T](
      generator:   Gen[T],
      minElements: Int Refined Positive = 1,
      maxElements: Int Refined Positive = 5
  ): Gen[Set[T]] =
    for {
      size <- choose(minElements.value, maxElements.value)
      set  <- Gen.containerOfN[Set, T](size, generator)
    } yield set

  def listOf[T](generator:   Gen[T],
                minElements: Int Refined NonNegative = 0,
                maxElements: Int Refined Positive = 5
  ): Gen[List[T]] =
    for {
      size <- choose(minElements.value, maxElements.value)
      list <- Gen.listOfN(size, generator)
    } yield list

  def setOf[T](generator:   Gen[T],
               minElements: Int Refined NonNegative = 0,
               maxElements: Int Refined Positive = 5
  ): Gen[Set[T]] = {
    require(minElements.value <= maxElements.value)

    for {
      size <- choose(minElements.value, maxElements.value)
      set  <- Gen.containerOfN[Set, T](size, generator)
    } yield set
  }

  lazy val booleans: Gen[Boolean] = Gen.oneOf(true, false)

  def positiveInts(max: Int = 1000): Gen[Int Refined Positive] =
    choose(1, max) map Refined.unsafeApply

  def ints(min: Int = Int.MinValue, max: Int = Int.MaxValue): Gen[Int] = choose(min, max)

  def positiveLongs(max: Long = 1000): Gen[Long Refined Positive] =
    choose(1L, max) map Refined.unsafeApply

  def nonNegativeFloats(max: Float = 1000): Gen[Float Refined NonNegative] = choose(0, max) map Refined.unsafeApply

  def nonNegativeDoubles(max: Double = 1000d): Gen[Double Refined NonNegative] = choose(0d, max) map Refined.unsafeApply

  def negativeDoubles(min: Double = -1000d): Gen[Double Refined Negative] = choose(min, 0d) map Refined.unsafeApply

  def nonNegativeInts(max: Int = 1000): Gen[Int Refined NonNegative] = choose(0, max) map Refined.unsafeApply

  def nonPositiveInts(min: Int = -1000): Gen[Int Refined NonPositive] = choose(min, 0) map Refined.unsafeApply

  def negativeInts(min: Int = -1000): Gen[Int] = choose(min, -1)

  def nonNegativeLongs(max: Long = 1000): Gen[Long Refined NonNegative] = choose(0L, max) map Refined.unsafeApply

  def durations(min: FiniteDuration = 0 millis, max: FiniteDuration = 5 seconds): Gen[FiniteDuration] =
    choose(min.toMillis, max.toMillis) map (FiniteDuration(_, MILLISECONDS).toCoarsest)

  def relativePaths(minSegments: Int = 1,
                    maxSegments: Int = 10,
                    partsGenerator: Gen[String] = nonBlankStrings(
                      charsGenerator = frequency(9 -> alphaChar, 1 -> oneOf('-', '_')),
                      minLength = 3
                    ).map(_.value)
  ): Gen[String] = {
    require(minSegments <= maxSegments,
            s"Generate relative paths with minSegments=$minSegments and maxSegments=$maxSegments makes no sense"
    )

    for {
      partsNumber <- Gen.choose(minSegments, maxSegments)
      parts       <- Gen.listOfN(partsNumber, partsGenerator)
    } yield parts.mkString("/")
  }

  val httpPorts: Gen[Int Refined Positive] = choose(2000, 10000) map Refined.unsafeApply

  def httpUrls(pathGenerator: Gen[String] = relativePaths(minSegments = 0, maxSegments = 2)): Gen[String] =
    for {
      protocol <- Arbitrary.arbBool.arbitrary map {
                    case true  => "http"
                    case false => "https"
                  }
      port <- httpPorts
      host <- nonEmptyStrings()
      path <- pathGenerator
      pathValidated = if (path.isEmpty) "" else s"/$path"
    } yield s"$protocol://$host:$port$pathValidated"

  val localHttpUrls: Gen[String] = for {
    protocol <- Arbitrary.arbBool.arbitrary map {
                  case true  => "http"
                  case false => "https"
                }
    port <- httpPorts
  } yield s"$protocol://localhost:$port"

  lazy val semanticVersions: Gen[String] = Gen.listOfN(3, positiveInts(max = 150)).map(_.mkString("."))

  val validatedUrls: Gen[String Refined Url] = httpUrls() map Refined.unsafeApply

  val shas: Gen[String] = for {
    length <- Gen.choose(40, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'f').map(_.toString)))
  } yield chars.mkString("")

  val timestamps: Gen[Instant] = timestamps()

  def timestamps(
      min: Instant = Instant.EPOCH,
      max: Instant = now().plus(2000, JAVA_DAYS)
  ): Gen[Instant] = Gen
    .choose(min.toEpochMilli, max.toEpochMilli)
    .map(Instant.ofEpochMilli)

  def relativeTimestamps(
      lessThanAgo: JavaDuration = JavaDuration.ofDays(365 * 5),
      moreThanAgo: JavaDuration = JavaDuration.ZERO
  ): Gen[Instant] = {
    if ((lessThanAgo compareTo moreThanAgo) < 0)
      throw new IllegalArgumentException(
        s"relativeTimestamps with lessThanAgo = $lessThanAgo and moreThanAgo = $moreThanAgo"
      )

    timestamps(min = now.minus(lessThanAgo), max = now.minus(moreThanAgo))
  }

  val timestampsNotInTheFuture: Gen[Instant] =
    Gen
      .choose(Instant.EPOCH.toEpochMilli, now().toEpochMilli)
      .map(Instant.ofEpochMilli)

  def timestampsNotInTheFuture(butYoungerThan: Instant): Gen[Instant] =
    Gen
      .choose((butYoungerThan plusMillis 1).toEpochMilli, now().toEpochMilli)
      .map(Instant.ofEpochMilli)

  val timestampsInTheFuture: Gen[Instant] =
    Gen
      .choose(now().plus(10, JAVA_MINS).toEpochMilli, now().plus(2000, JAVA_DAYS).toEpochMilli)
      .map(Instant.ofEpochMilli)

  val zonedDateTimes: Gen[ZonedDateTime] =
    timestamps
      .map(ZonedDateTime.ofInstant(_, ZoneId.of(ZoneOffset.UTC.getId)))

  val localDates: Gen[LocalDate] =
    timestamps
      .map(LocalDateTime.ofInstant(_, ZoneOffset.UTC))
      .map(_.toLocalDate)

  val localDatesNotInTheFuture: Gen[LocalDate] =
    timestampsNotInTheFuture
      .map(LocalDateTime.ofInstant(_, ZoneOffset.UTC))
      .map(_.toLocalDate)

  val notNegativeJavaDurations: Gen[JavaDuration] = javaDurations(min = 0, max = 20000)

  def javaDurations(
      min: JavaDuration = JavaDuration.ZERO,
      max: JavaDuration = JavaDuration.ofDays(20)
  ): Gen[JavaDuration] = javaDurations(min.toMillis, max.toMillis)

  def javaDurations(min: Long, max: Long): Gen[JavaDuration] =
    Gen
      .choose(min, max)
      .map(JavaDuration.ofMillis)

  implicit val exceptions: Gen[Exception] = nonEmptyStrings(20) map (new Exception(_))
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
      value <- oneOf(nonEmptyStrings(maxLength = 5),
                     Arbitrary.arbNumber.arbitrary,
                     Arbitrary.arbBool.arbitrary,
                     Gen.nonEmptyListOf(nonEmptyStrings())
               )
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

      def generateOne: T = generateExample(generator)

      def generateAs[TT](implicit ttFactory: T => TT): TT =
        generateExample(generator map ttFactory)

      def generateFixedSizeList(ofSize: Int Refined Positive): List[T] =
        generateNonEmptyList(minElements = ofSize, maxElements = ofSize).toList

      def generateList(minElements: Int Refined NonNegative = 0, maxElements: Int Refined Positive = 5): List[T] =
        generateExample(listOf(generator, minElements, maxElements))

      def generateFixedSizeSet(ofSize: Int Refined Positive = 5): Set[T] =
        generateExample(setOf(generator, minElements = ofSize, maxElements = ofSize))

      def generateSet(minElements: Int Refined NonNegative = 0, maxElements: Int Refined Positive = 5): Set[T] =
        generateExample(setOf(generator, minElements, maxElements))

      def generateNonEmptyList(minElements: Int Refined Positive = 1,
                               maxElements: Int Refined Positive = 5
      ): NonEmptyList[T] =
        generateExample(nonEmptyList(generator, minElements, maxElements))

      def generateOption: Option[T] = Gen.option(generator).sample getOrElse generateOption

      def generateSome: Option[T] = Option(generator.generateOne)

      def generateNone: Option[T] = Option.empty

      def generateDifferentThan(value: T): T = {
        val generated = generator.sample.getOrElse(generateDifferentThan(value))
        if (generated == value) generateDifferentThan(value)
        else generated
      }

      def toGenerateOfFixedValue: Gen[T] = fixed(generateExample(generator))

      def toGeneratorOfSomes:   Gen[Option[T]] = generator map Option.apply
      def toGeneratorOfNones:   Gen[Option[T]] = Gen.const(None)
      def toGeneratorOfOptions: Gen[Option[T]] = Gen.option(generator)
      def toGeneratorOfNonEmptyList(minElements: Int Refined Positive = 1,
                                    maxElements: Int Refined Positive = 5
      ): Gen[NonEmptyList[T]] = nonEmptyList(generator, minElements, maxElements)

      def toGeneratorOfList(minElements: Int Refined NonNegative = 0,
                            maxElements: Int Refined Positive = 5
      ): Gen[List[T]] = listOf(generator, minElements, maxElements)

      def toGeneratorOfSet(minElements: Int Refined NonNegative = 1,
                           maxElements: Int Refined Positive = 5
      ): Gen[Set[T]] =
        setOf(generator, minElements, maxElements)

      def toGeneratorOf[TT](implicit ttFactory: T => TT): Gen[TT] = generator map ttFactory

      private def generateExample[O](generator: Gen[O]): O =
        generator.sample getOrElse generateExample(generator)
    }

    implicit def asArbitrary[T](implicit generator: Gen[T]): Arbitrary[T] = Arbitrary(generator)

    implicit val semigroupalGen: Semigroupal[Gen] = new Semigroupal[Gen] {
      override def product[A, B](fa: Gen[A], fb: Gen[B]): Gen[(A, B)] = for {
        a <- fa
        b <- fb
      } yield a -> b
    }

    implicit val genApplicative: Applicative[Gen] = new Applicative[Gen] {
      override def pure[A](x: A) = Gen.const(x)
      override def ap[A, B](ff: Gen[A => B])(fa: Gen[A]): Gen[B] = ff.flatMap(f => fa.map(f))
    }

    implicit val functorGen: Functor[Gen] = new Functor[Gen] {
      override def map[A, B](fa: Gen[A])(f: A => B): Gen[B] = fa.map(f)
    }
  }
}
