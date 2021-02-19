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

package io.renku.eventlog.subscriptions.zombieevents

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import io.circe.literal._
import io.renku.eventlog.subscriptions.EventEncoder
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ZombieEventEncoderSpec extends AnyWordSpec with should.Matchers {

  "encodeEvent" should {

    "serialize ZombieEvent to Json" in {
      val event = ZombieEvent(compoundEventIds.generateOne, projectPaths.generateOne, eventStatuses.generateOne)

      encoder.encodeEvent(event) shouldBe json"""{
        "categoryName": "ZOMBIE_CHASING",
        "id":           ${event.eventId.id.value},
        "project": {
          "id":         ${event.eventId.projectId.value},
          "path":       ${event.projectPath.value}
        },
        "status":       ${event.status.value}
      }"""
    }
  }

  "encodePayload" should {

    "return None" in {
      encoder.encodePayload(
        ZombieEvent(compoundEventIds.generateOne, projectPaths.generateOne, eventStatuses.generateOne)
      ) shouldBe None
    }
  }

  private lazy val encoder: EventEncoder[ZombieEvent] = ZombieEventEncoder
}
