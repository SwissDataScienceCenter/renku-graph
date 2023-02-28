/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers.awaitinggeneration

import cats.effect.IO
import io.renku.eventlog.events.producers.{CapacityFinder, CapacityFindingQuerySpec, UsedCapacity}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus.GeneratingTriples
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscriptionCategorySpec extends AnyWordSpec with should.Matchers with CapacityFindingQuerySpec {

  "capacityFindingQuery" should {

    s"count events in the $GeneratingTriples status" in {

      createEvent(EventStatus.GeneratingTriples)
      createEvent(Gen.oneOf(EventStatus.all - EventStatus.GeneratingTriples).generateOne)

      CapacityFinder
        .queryBased[IO](SubscriptionCategory.capacityFindingQuery)
        .findUsedCapacity
        .unsafeRunSync() shouldBe UsedCapacity(1)
    }
  }
}
