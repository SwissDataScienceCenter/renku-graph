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
import cats.syntax.all._
import cats.{Functor, Monad, Semigroupal}
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

  def randomiseCases(value: String): Gen[String] = {
    for {
      itemsNo      <- positiveInts(value.length).map(_.value)
      itemsIndices <- Gen.pick(itemsNo, value.zipWithIndex.map(_._2))
    } yield value.zipWithIndex.foldLeft(List.empty[Char]) {
      case (newChars, char -> idx) if itemsIndices contains idx =>
        (if (char.isLower) char.toUpper else char.toLower) :: newChars
      case (newChars, char -> _) => char :: newChars
    }
  }.map(_.reverse.mkString)

  def nonEmptyStrings(minLength: Int = 1, maxLength: Int = 10, charsGenerator: Gen[Char] = alphaChar): Gen[String] =
    nonBlankStrings(minLength, maxLength, charsGenerator) map (_.value)

  def nonBlankStrings(minLength: Int = 1, maxLength: Int = 10, charsGenerator: Gen[Char] = alphaChar): Gen[NonBlank] = {
    require(minLength <= maxLength)

    val lengths =
      if (maxLength == 1) const(maxLength)
      else if (minLength == maxLength) const(maxLength)
      else frequency(1 -> choose(minLength, maxLength), 9 -> choose(minLength + 1, maxLength))

    for {
      length <- lengths
      chars  <- listOfN(length, charsGenerator)
    } yield Refined.unsafeApply(chars.mkString(""))
  }

  def stringsOfLength(length: Int = 10, charsGenerator: Gen[Char] = alphaChar): Gen[String] =
    listOfN(length, charsGenerator).map(_.mkString(""))

  def paragraphs(minElements: Int = 5, maxElements: Int = 10): Gen[NonBlank] =
    nonEmptyStringsList(minElements, maxElements) map (_.mkString(" ")) map Refined.unsafeApply

  def sentences(minWords: Int = 1, maxWords: Int = 10, charsGenerator: Gen[Char] = alphaChar): Gen[NonBlank] = {
    require(minWords <= maxWords, s"minWords = $minWords has to be > maxWords = $maxWords")

    nonEmptyStringsList(minWords, maxWords, charsGenerator) map (_.mkString(" ")) map Refined.unsafeApply
  }

  def sentenceContaining(phrase: NonBlank): Gen[String] = for {
    prefix <- nonEmptyStrings(minLength = 3)
    suffix <- nonEmptyStrings(minLength = 3)
  } yield s"$prefix $phrase $suffix"

  def blankStrings(maxLength: Int = 10): Gen[String] = for {
    length <- choose(0, maxLength)
    chars  <- listOfN(length, const(" "))
  } yield chars.mkString("")

  def nonEmptyStringsList(min: Int = 1, max: Int = 5, charsGenerator: Gen[Char] = alphaChar): Gen[List[String]] = {
    require(min <= max, s"min = $min has to be > max = $max")

    for {
      size  <- choose(min, max)
      lines <- Gen.listOfN(size, nonEmptyStrings(charsGenerator = charsGenerator))
    } yield lines
  }

  def nonEmptyList[T](generator: Gen[T], min: Int = 1, max: Int = 5): Gen[NonEmptyList[T]] = {
    require(min <= max, s"min = $min has to be > max = $max")

    for {
      size <- choose(min, max)
      list <- Gen.listOfN(size, generator)
    } yield NonEmptyList.fromListUnsafe(list)
  }

  def nonEmptySet[T](generator: Gen[T], min: Int = 1, max: Int = 5): Gen[Set[T]] = {
    require(min <= max, s"min = $min has to be > max = $max")

    for {
      size <- choose(min, max)
      set  <- Gen.containerOfN[Set, T](size, generator)
    } yield set
  }

  def listOf[T](generator: Gen[T], min: Int = 0, max: Int = 5): Gen[List[T]] = {
    require(min <= max, s"min = $min has to be > max = $max")

    for {
      size <- choose(min, max)
      list <- Gen.listOfN(size, generator)
    } yield list
  }

  def setOf[T](generator: Gen[T], min: Int = 0, max: Int = 5): Gen[Set[T]] = {
    require(min <= max, s"min = $min has to be > max = $max")

    for {
      size <- choose(min, max)
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

  def periods(min: LocalDate = LocalDate.EPOCH, max: LocalDate = LocalDate.now().plus(2000, JAVA_DAYS)): Gen[Period] =
    Period.between(min, max)

  def relativePaths(minSegments: Int = 1,
                    maxSegments: Int = 10,
                    partsGenerator: Gen[String] = nonBlankStrings(
                      charsGenerator = frequency(9 -> alphaChar, 1 -> oneOf('-', '_')),
                      minLength = 3
                    ).map(_.value)
  ): Gen[String] = {
    require(minSegments <= maxSegments,
            s"Generate relative paths with minSegments = $minSegments and maxSegments = $maxSegments makes no sense"
    )

    for {
      partsNumber <- Gen.choose(minSegments, maxSegments)
      parts       <- Gen.listOfN(partsNumber, partsGenerator)
    } yield parts.mkString("/")
  }

  val httpPorts: Gen[Int] = choose(2000, 10000)

  def httpUrls(hostGenerator: Gen[String] = nonEmptyStrings(),
               pathGenerator: Gen[String] = relativePaths(minSegments = 0, maxSegments = 2)
  ): Gen[String] = for {
    protocol <- Gen.oneOf("http", "https")
    port     <- httpPorts
    host     <- hostGenerator
    path     <- pathGenerator
    pathValidated = if (path.isEmpty) "" else s"/$path"
  } yield s"$protocol://$host:$port$pathValidated"

  val localHttpUrls: Gen[String] = for {
    protocol <- Gen.oneOf("http", "https")
    port     <- httpPorts
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

    timestamps(min = now minus lessThanAgo, max = now minus moreThanAgo)
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

  def localDates(min: LocalDate = LocalDate.EPOCH,
                 max: LocalDate = LocalDate.now().plus(2000, JAVA_DAYS)
  ): Gen[LocalDate] = Gen
    .choose(min.toEpochDay, max.toEpochDay)
    .map(LocalDate.ofEpochDay)

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

  implicit val exceptions: Gen[Exception] = nonEmptyStrings(maxLength = 20) map (new Exception(_))
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
      value <- oneOf[Any](nonEmptyStrings(maxLength = 5),
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

      def generateFixedSizeList(ofSize: Int): List[T] =
        generateNonEmptyList(min = ofSize, max = ofSize).toList

      def generateList(min: Int = 0, max: Int = 5): List[T] =
        generateExample(listOf(generator, min, max))

      def generateFixedSizeSet(ofSize: Int = 5): Set[T] =
        generateExample(setOf(generator, min = ofSize, max = ofSize))

      def generateSet(min: Int = 0, max: Int = 5): Set[T] =
        generateExample(setOf(generator, min, max))

      def generateNonEmptyList(min: Int = 1, max: Int = 5): NonEmptyList[T] =
        generateExample(nonEmptyList(generator, min, max))

      def generateOption: Option[T] = Gen.option(generator).sample getOrElse generateOption

      def generateSome: Option[T] = Option(generator.generateOne)

      def generateNone: Option[T] = Option.empty

      def generateRight[A]: Either[A, T] = generateExample(generator).asRight[A]

      def generateLeft[B]: Either[T, B] = generateExample(generator).asLeft[B]

      def generateDifferentThan(value: T): T = {
        val generated = generator.sample.getOrElse(generateDifferentThan(value))
        if (generated == value) generateDifferentThan(value)
        else generated
      }

      def toGenerateOfFixedValue: Gen[T] = fixed(generateExample(generator))

      def toGeneratorOfSomes:   Gen[Option[T]] = generator map Option.apply
      def toGeneratorOfNones:   Gen[Option[T]] = Gen.const(None)
      def toGeneratorOfOptions: Gen[Option[T]] = frequency(3 -> const(None), 7 -> some(generator))
      def toGeneratorOfNonEmptyList(minElements: Int Refined Positive = 1,
                                    maxElements: Int Refined Positive = 5
      ): Gen[NonEmptyList[T]] = nonEmptyList(generator, minElements, maxElements)

      def toGeneratorOfList(min: Int = 0, max: Int = 5): Gen[List[T]] = listOf(generator, min, max)

      def toGeneratorOfSet(min: Int = 1, max: Int = 5): Gen[Set[T]] =
        setOf(generator, min, max)

      def toGeneratorOf[TT](implicit ttFactory: T => TT): Gen[TT] = generator map ttFactory

      private def generateExample[O](generator: Gen[O]): O = {
        @annotation.tailrec
        def loop(tries: Int): O =
          generator.sample match {
            case Some(o)               => o
            case None if tries >= 5000 => sys.error(s"Failed to generate example value after $tries tries")
            case None                  => loop(tries + 1)
          }

        loop(0)
      }
    }

    implicit def asArbitrary[T](implicit generator: Gen[T]): Arbitrary[T] = Arbitrary(generator)

    implicit val semigroupalGen: Semigroupal[Gen] = new Semigroupal[Gen] {
      override def product[A, B](fa: Gen[A], fb: Gen[B]): Gen[(A, B)] = for {
        a <- fa
        b <- fb
      } yield a -> b
    }

    implicit val genMonad: Monad[Gen] = new Monad[Gen] {
      override def pure[A](x:        A) = Gen.const(x)
      override def ap[A, B](ff:      Gen[A => B])(fa: Gen[A]):      Gen[B] = ff.flatMap(f => fa.map(f))
      override def flatMap[A, B](fa: Gen[A])(f:       A => Gen[B]): Gen[B] = fa.flatMap(f)
      override def tailRecM[A, B](a: A)(f: A => Gen[Either[A, B]]): Gen[B] = f(a).flatMap {
        case Left(aValue)  => tailRecM[A, B](aValue)(f)
        case Right(bValue) => fixed(bValue)
      }
    }

    implicit val functorGen: Functor[Gen] = new Functor[Gen] {
      override def map[A, B](fa: Gen[A])(f: A => B): Gen[B] = fa.map(f)
    }
  }
}
