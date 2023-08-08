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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.effect.IO
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{CleanUpEventsProvisioning, InMemoryEventLogDbSpec}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs}
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.OffsetDateTime

class CleanUpEventsQueueSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with CleanUpEventsProvisioning
    with MockFactory
    with should.Matchers {

  "offer" should {

    "insert a new row if there's no row for the project" in new TestCase {
      val id1   = projectIds.generateOne
      val slug1 = projectSlugs.generateOne
      val id2   = projectIds.generateOne
      val slug2 = projectSlugs.generateOne

      queue.offer(id1, slug1).unsafeRunSync() shouldBe ()
      queue.offer(id2, slug2).unsafeRunSync() shouldBe ()

      findCleanUpEvents shouldBe List(id1 -> slug1, id2 -> slug2)
    }

    "do not insert duplicates" in new TestCase {
      val id   = projectIds.generateOne
      val slug = projectSlugs.generateOne

      queue.offer(id, slug).unsafeRunSync() shouldBe ()
      queue.offer(id, slug).unsafeRunSync() shouldBe ()

      findCleanUpEvents shouldBe List(id -> slug)
    }
  }

  private trait TestCase {
    val now = OffsetDateTime.now()

    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val currentTime = mockFunction[OffsetDateTime]
    val queue       = new CleanUpEventsQueueImpl[IO](currentTime)

    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
