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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{CleanUpEventsProvisioning, EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.OffsetDateTime

class CleanUpEventsQueueSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with CleanUpEventsProvisioning
    with should.Matchers {

  "offer" should {

    "insert a new row if there's no row for the project" in testDBResource.use { implicit cfg =>
      val project1 = consumerProjects.generateOne
      val project2 = consumerProjects.generateOne

      for {
        _ <- queue.offer(project1.id, project1.slug).assertNoException
        _ <- queue.offer(project2.id, project2.slug).assertNoException

        _ <- findCleanUpEvents.asserting(_ shouldBe List(project1, project2))
      } yield Succeeded
    }

    "do not insert duplicates" in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne

      for {
        _ <- queue.offer(project.id, project.slug).assertNoException
        _ <- queue.offer(project.id, project.slug).assertNoException

        _ <- findCleanUpEvents.asserting(_ shouldBe List(project))
      } yield Succeeded
    }
  }

  private lazy val now = OffsetDateTime.now()
  private def queue(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new CleanUpEventsQueueImpl[IO](() => now)
  }
}
