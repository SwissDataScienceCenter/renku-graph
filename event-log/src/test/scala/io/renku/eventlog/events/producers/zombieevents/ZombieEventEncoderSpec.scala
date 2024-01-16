/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers.zombieevents

import Generators.zombieEvents
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ZombieEventEncoderSpec extends AnyFlatSpec with should.Matchers {

  it should "serialize ZombieEvent to Json" in {
    val event = zombieEvents.generateOne

    ZombieEventEncoder.encodeEvent(event) shouldBe
      json"""{
        "categoryName": "ZOMBIE_CHASING",
        "id":           ${event.eventId.id},
        "project": {
          "id":   ${event.eventId.projectId},
          "slug": ${event.projectSlug}
        },
        "status": ${event.status}
      }"""
  }
}
