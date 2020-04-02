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

package ch.datascience.graph.model

import java.time.{Clock, Instant, ZoneId}

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.tinytypes.constraints.NonBlank
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CommitEventIdSpec extends WordSpec {

  "toString" should {

    "be of format 'id = <eventId>, projectId = <projectId>'" in {
      val commitEventId = compoundEventIds.generateOne

      commitEventId.toString shouldBe s"id = ${commitEventId.id}, projectId = ${commitEventId.projectId}"
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

class CompoundEventIdSpec extends WordSpec with ScalaCheckPropertyChecks {

  "toString" should {

    "be of format 'id = <eventId>, projectId = <projectId>'" in {
      forAll { commitEventId: CompoundEventId =>
        commitEventId.toString shouldBe s"id = ${commitEventId.id}, projectId = ${commitEventId.projectId}"
      }
    }
  }
}

class BatchDateSpec extends WordSpec {

  "apply()" should {

    "instantiate a new BatchDate with current timestamp" in {
      val systemZone = ZoneId.systemDefault
      val fixedNow   = Instant.now

      val clock = Clock.fixed(fixedNow, systemZone)

      BatchDate(clock).value shouldBe fixedNow

      Clock.system(systemZone)
    }
  }
}
