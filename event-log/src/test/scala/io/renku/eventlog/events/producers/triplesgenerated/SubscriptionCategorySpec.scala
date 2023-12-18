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

package io.renku.eventlog.events.producers.triplesgenerated

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.eventlog.EventLogPostgresSpec
import io.renku.eventlog.events.producers.{CapacityFinder, UsedCapacity}
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus.TransformingTriples
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class SubscriptionCategorySpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  "capacityFindingQuery" should {

    s"count events in the $TransformingTriples status" in testDBResource.use { implicit cfg =>
      for {
        _ <- storeGeneratedEvent(status = EventStatus.TransformingTriples)
        _ <- storeGeneratedEvent(status = Gen.oneOf(EventStatus.all - EventStatus.TransformingTriples).generateOne)

        _ <- CapacityFinder
               .ofStatus[IO](EventStatus.TransformingTriples)
               .findUsedCapacity
               .asserting(_ shouldBe UsedCapacity(1))
      } yield Succeeded
    }
  }

  private implicit lazy val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
}
