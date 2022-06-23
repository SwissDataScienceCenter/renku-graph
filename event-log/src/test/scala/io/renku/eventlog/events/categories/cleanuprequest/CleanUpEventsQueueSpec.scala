/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.categories.cleanuprequest

import cats.effect.IO
import io.renku.db.SqlStatement
import io.renku.eventlog.{CleanUpEventsProvisioning, InMemoryEventLogDbSpec}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.metrics.TestLabeledHistogram
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
      val path1 = projectPaths.generateOne
      val id2   = projectIds.generateOne
      val path2 = projectPaths.generateOne

      queue.offer(id1, path1).unsafeRunSync() shouldBe ()
      queue.offer(id2, path2).unsafeRunSync() shouldBe ()

      findCleanUpEvents shouldBe List(id1 -> path1, id2 -> path2)
    }

    "do not insert duplicates" in new TestCase {
      val id   = projectIds.generateOne
      val path = projectPaths.generateOne

      queue.offer(id, path).unsafeRunSync() shouldBe ()
      queue.offer(id, path).unsafeRunSync() shouldBe ()

      findCleanUpEvents shouldBe List(id -> path)
    }
  }

  private trait TestCase {
    val now = OffsetDateTime.now()

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val currentTime      = mockFunction[OffsetDateTime]
    val queue            = new CleanUpEventsQueueImpl[IO](queriesExecTimes, currentTime)

    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
