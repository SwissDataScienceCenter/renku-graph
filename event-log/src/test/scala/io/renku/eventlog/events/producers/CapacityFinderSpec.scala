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

package io.renku.eventlog.events.producers

import cats.Id
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.EventStatus
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CapacityFinderSpec extends AnyWordSpec with should.Matchers with CapacityFindingQuerySpec {

  "noOpCapacityFinder" should {

    "always return Capacity.zero" in {
      CapacityFinder.noOpCapacityFinder[Id].findUsedCapacity shouldBe UsedCapacity.zero
    }
  }

  "QueryBasedCapacityFinder" should {

    "return used capacity using the given query" in {

      createEvent(EventStatus.GeneratingTriples)
      createEvent(Gen.oneOf(EventStatus.all - EventStatus.GeneratingTriples).generateOne)

      val finder = CapacityFinder
        .queryBased[IO](s"SELECT COUNT(event_id) FROM event WHERE status='${EventStatus.GeneratingTriples.value}'")

      finder.findUsedCapacity.unsafeRunSync() shouldBe UsedCapacity(1)

      createEvent(EventStatus.GeneratingTriples)

      finder.findUsedCapacity.unsafeRunSync() shouldBe UsedCapacity(2)
    }
  }
}