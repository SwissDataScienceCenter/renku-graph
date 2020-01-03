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

package ch.datascience.dbeventlog

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.constraints.{InstantNotInTheFuture, NonBlank}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EventStatusSpec extends WordSpec with ScalaCheckPropertyChecks {

  import EventStatus._

  "EventStatus" should {

    val scenarios = Table(
      "String Value"            -> "Expected EventStatus",
      "NEW"                     -> New,
      "PROCESSING"              -> Processing,
      "TRIPLES_STORE"           -> TriplesStore,
      "TRIPLES_STORE_FAILURE"   -> TriplesStoreFailure,
      "NON_RECOVERABLE_FAILURE" -> NonRecoverableFailure
    )

    forAll(scenarios) { (stringValue, expectedStatus) =>
      s"be instantiatable from '$stringValue'" in {
        EventStatus.from(stringValue) shouldBe Right(expectedStatus)
      }
    }

    "fail for unknown value" in {
      val unknown = nonEmptyStrings().generateOne

      val Left(exception) = EventStatus.from(unknown)

      exception.getMessage shouldBe s"'$unknown' unknown EventStatus"
    }
  }
}

class EventBodySpec extends WordSpec with ScalaCheckPropertyChecks {

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
}

class CreatedDateSpec extends WordSpec with ScalaCheckPropertyChecks {

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

class ExecutionDateSpec extends WordSpec with ScalaCheckPropertyChecks {

  "ExecutionDate" should {

    "be instantiatable from any Instant" in {
      forAll(timestamps) { instant =>
        ExecutionDate.from(instant).map(_.value) shouldBe Right(instant)
      }
    }
  }
}

class EventMessageSpec extends WordSpec with ScalaCheckPropertyChecks {

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
