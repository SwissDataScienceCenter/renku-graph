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

package ch.datascience.graph.model

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events._
import ch.datascience.tinytypes.constraints.{DurationNotNegative, NonBlank}
import io.circe.Json
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.temporal.ChronoUnit.{HOURS, SECONDS}
import java.time.{Clock, Instant, ZoneId, Duration => JavaDuration}

class EventStatusSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "EventStatus" should {

    val scenarios = Table(
      "String Value"                           -> "Expected EventStatus",
      "NEW"                                    -> New,
      "GENERATING_TRIPLES"                     -> GeneratingTriples,
      "TRIPLES_STORE"                          -> TriplesStore,
      "SKIPPED"                                -> Skipped,
      "GENERATION_RECOVERABLE_FAILURE"         -> GenerationRecoverableFailure,
      "GENERATION_NON_RECOVERABLE_FAILURE"     -> GenerationNonRecoverableFailure,
      "TRANSFORMATION_RECOVERABLE_FAILURE"     -> TransformationRecoverableFailure,
      "TRANSFORMATION_NON_RECOVERABLE_FAILURE" -> TransformationNonRecoverableFailure
    )

    forAll(scenarios) { (stringValue, expectedStatus) =>
      s"be instantiatable from '$stringValue'" in {
        EventStatus.from(stringValue) shouldBe Right(expectedStatus)
      }

      s"be deserializable from $stringValue" in {
        Json.fromString(stringValue).as[EventStatus] shouldBe Right(expectedStatus)
      }
    }

    "fail instantiation for unknown value" in {
      val unknown = nonEmptyStrings().generateOne

      val Left(exception) = EventStatus.from(unknown)

      exception.getMessage shouldBe s"'$unknown' unknown EventStatus"
    }

    "fail deserialization for unknown value" in {
      val unknown = nonEmptyStrings().generateOne

      val Left(exception) = Json.fromString(unknown).as[EventStatus]

      exception.getMessage shouldBe s"'$unknown' unknown EventStatus"
    }
  }
}

class CompoundEventIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "toString" should {

    "be of format 'id = <eventId>, projectId = <projectId>'" in {
      forAll { commitEventId: CompoundEventId =>
        commitEventId.toString shouldBe s"id = ${commitEventId.id}, projectId = ${commitEventId.projectId}"
      }
    }
  }
}

class CommittedDateSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "instantiation" should {

    "succeed if less than a day in the future" in {
      forAll(timestamps(max = Instant.now().plus(24, HOURS).minus(1, SECONDS))) { value =>
        CommittedDate.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail if further than a day in the future" in {
      forAll(timestamps(min = Instant.now().plus(24, HOURS).plus(1, SECONDS))) { value =>
        val Left(exception) = CommittedDate.from(value).map(_.value)
        exception          shouldBe an[IllegalArgumentException]
        exception.getMessage should startWith(s"${CommittedDate.typeName} has to be <= ")
      }
    }
  }
}

class EventBodySpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  import io.circe.Decoder
  import io.circe.literal._

  "EventBody" should {

    "have the NonBlank constraint" in {
      EventBody shouldBe an[NonBlank]
    }

    "be instantiatable from any non-blank string" in {
      forAll(nonEmptyStrings()) { body =>
        EventBody.from(body).map(_.value) shouldBe Right(body)
      }
    }
  }

  "decodeAs" should {

    "parse the string value into Json and decode using the given decoder" in {
      val value = nonBlankStrings().generateOne.value

      case class Wrapper(value: String)
      implicit val decoder: Decoder[Wrapper] = Decoder.instance[Wrapper] {
        _.downField("field").as[String].map(Wrapper.apply)
      }

      val eventBody = EventBody {
        json"""{
          "field": $value
        }""".noSpaces
      }

      eventBody.decodeAs[Wrapper] shouldBe Right(Wrapper(value))
    }
  }
}

class BatchDateSpec extends AnyWordSpec with should.Matchers {

  "apply()" should {

    "instantiate a new BatchDate with current timestamp" in {
      val fixedNow = Instant.now

      val clock = Clock.fixed(fixedNow, ZoneId.systemDefault())

      BatchDate(clock).value shouldBe fixedNow
    }
  }
}

class EventProcessingTimeSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "EventProcessingTime" should {

    "have the DurationNotNegative constraint" in {
      EventProcessingTime shouldBe an[DurationNotNegative]
    }

    "be instantiatable from any non negative finite durations" in {
      forAll(notNegativeJavaDurations) { body =>
        EventProcessingTime.from(body).map(_.value) shouldBe Right(body)
      }
    }

    "throw an error if it is instantiated with a negative finite duration" in {
      forAll(
        javaDurations(min = -2000, max = -1)
      ) { duration =>
        val Left(exception) = EventProcessingTime.from(duration).map(_.value)
        exception          shouldBe an[IllegalArgumentException]
        exception.getMessage should startWith(s"${EventProcessingTime.typeName} cannot have a negative duration")
      }
    }
  }

  "*" should {

    "multiply the given processing time by the given value" in {
      forAll(eventProcessingTimes, positiveInts()) { (processingTime, multiplier) =>
        processingTime * multiplier shouldBe EventProcessingTime(
          JavaDuration.ofMillis(processingTime.value.toMillis * multiplier.value)
        )
      }
    }
  }

  "/" should {

    "divide the given processing time by the given value" in {
      forAll(eventProcessingTimes, positiveInts()) { (processingTime, multiplier) =>
        processingTime / multiplier shouldBe EventProcessingTime(
          JavaDuration.ofMillis(processingTime.value.toMillis / multiplier.value)
        )
      }
    }
  }
}
