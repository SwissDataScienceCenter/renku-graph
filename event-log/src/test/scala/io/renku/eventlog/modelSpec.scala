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

package io.renku.eventlog

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.tinytypes.constraints.{InstantNotInTheFuture, NonBlank}
import io.circe.Json
import io.renku.eventlog.DbEventLogGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EventStatusSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  import EventStatus._

  "EventStatus" should {

    val scenarios = Table(
      "String Value"            -> "Expected EventStatus",
      "NEW"                     -> New,
      "PROCESSING"              -> Processing,
      "TRIPLES_STORE"           -> TriplesStore,
      "SKIPPED"                 -> Skipped,
      "RECOVERABLE_FAILURE"     -> RecoverableFailure,
      "NON_RECOVERABLE_FAILURE" -> NonRecoverableFailure
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

class CreatedDateSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "CreatedDate" should {

    "have the InstantNotInTheFuture constraint" in {
      CreatedDate shouldBe an[InstantNotInTheFuture]
    }

    "be instantiatable from any Instant not from the future" in {
      forAll(timestampsNotInTheFuture) { instant =>
        CreatedDate.from(instant).map(_.value) shouldBe Right(instant)
      }
    }
  }
}

class ExecutionDateSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "ExecutionDate" should {

    "be instantiatable from any Instant" in {
      forAll(timestamps) { instant =>
        ExecutionDate.from(instant).map(_.value) shouldBe Right(instant)
      }
    }
  }
}

class EventMessageSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "Message" should {

    "have the NonBlank constraint" in {
      EventMessage shouldBe an[NonBlank]
    }

    "be instantiatable from any non-blank string" in {
      forAll(nonEmptyStrings()) { body =>
        EventMessage.from(body).map(_.value) shouldBe Right(body)
      }
    }

    "be instantiatable from an exception and contain the stack trace" in {
      import java.io._

      forAll(nestedExceptions) { exception =>
        val exceptionAsString = new StringWriter
        exception.printStackTrace(new PrintWriter(exceptionAsString))
        exceptionAsString.flush()

        EventMessage(exception).map(_.value) shouldBe Some(exceptionAsString.toString)
      }
    }
  }
}

class EventSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "compoundEventId" should {

    "create a CompoundEventId from the event's id and project id" in {
      forAll { event: Event =>
        event.compoundEventId shouldBe CompoundEventId(event.id, event.project.id)
      }
    }
  }
}
