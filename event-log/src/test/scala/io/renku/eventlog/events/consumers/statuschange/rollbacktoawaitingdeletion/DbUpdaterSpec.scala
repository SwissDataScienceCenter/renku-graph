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

package io.renku.eventlog.events.consumers.statuschange
package rollbacktoawaitingdeletion

import SkunkExceptionsGenerators.postgresErrors
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent.RollbackToAwaitingDeletion
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestampsNotInTheFuture}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventStatuses}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.metrics.TestMetricsRegistry
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk.SqlState.DeadlockDetected

class DbUpdaterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues {

  "updateDB" should {

    s"change status of all the event in the $Deleting status of a given project to $AwaitingDeletion" in testDBResource
      .use { implicit cfg =>
        val project     = consumerProjects.generateOne
        val otherStatus = eventStatuses.filter(_ != Deleting).generateOne
        for {
          event1Id <- addEvent(Deleting, project)
          event2Id <- addEvent(otherStatus, project)
          event3Id <- addEvent(Deleting, project)

          _ <- moduleSessionResource
                 .useK(dbUpdater updateDB RollbackToAwaitingDeletion(project))
                 .asserting(_ shouldBe DBUpdateResults(project.slug, Deleting -> -2, AwaitingDeletion -> 2))
          _ <- findEvent(event1Id).asserting(_.value.status shouldBe AwaitingDeletion)
          _ <- findEvent(event2Id).asserting(_.value.status shouldBe otherStatus)
          _ <- findEvent(event3Id).asserting(_.value.status shouldBe AwaitingDeletion)
        } yield ()
      }
  }

  "onRollback" should {

    "retry on DeadlockDetected" in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne

      addEvent(Deleting, project) >>
        dbUpdater
          .onRollback(RollbackToAwaitingDeletion(project))
          .apply(postgresErrors(DeadlockDetected).generateOne)
          .asserting(_ shouldBe DBUpdateResults(project.slug, Deleting -> -1, AwaitingDeletion -> 1))
    }

    "not be defined for an exception different than DeadlockDetected" in testDBResource.use { implicit cfg =>
      IO {
        dbUpdater
          .onRollback(RollbackToAwaitingDeletion(consumerProjects.generateOne))
          .isDefinedAt(exceptions.generateOne) shouldBe false
      }
    }
  }

  private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
  private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
  private lazy val dbUpdater = new DbUpdater[IO]()

  private def addEvent(status: EventStatus, project: Project)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[CompoundEventId] = {
    val eventId = compoundEventIds(project.id).generateOne
    storeEvent(
      eventId,
      status,
      timestampsNotInTheFuture.generateAs(ExecutionDate),
      timestampsNotInTheFuture.generateAs(EventDate),
      eventBodies.generateOne,
      projectSlug = project.slug
    ).as(eventId)
  }
}
