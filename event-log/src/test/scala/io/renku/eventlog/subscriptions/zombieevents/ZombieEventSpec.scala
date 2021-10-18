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

import cats.implicits.toShow
import io.renku.eventlog.subscriptions.zombieevents.Generators.zombieEventProcessNames
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventStatuses}
import io.renku.graph.model.GraphModelGenerators.projectPaths
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ZombieEventSpec extends AnyWordSpec with should.Matchers {

  "show" should {

    "print out the id and status" in {
      val event = ZombieEvent(zombieEventProcessNames.generateOne,
                              compoundEventIds.generateOne,
                              projectPaths.generateOne,
                              eventStatuses.generateOne
      )
      event.show shouldBe s"ZombieEvent ${event.generatedBy} ${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status}"
    }
  }
}
