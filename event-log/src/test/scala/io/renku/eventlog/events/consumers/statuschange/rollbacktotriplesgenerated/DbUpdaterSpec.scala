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

package io.renku.eventlog.events.consumers.statuschange.rollbacktotriplesgenerated

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent.RollbackToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange.DBUpdateResults
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class DbUpdaterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  "updateDB" should {

    s"change the status of the given event from $TransformingTriples to $TriplesGenerated" in testDBResource.use {
      implicit cfg =>
        val project = consumerProjects.generateOne
        for {
          event      <- addEvent(TransformingTriples, project)
          otherEvent <- addEvent(TransformingTriples, project)

          _ <- moduleSessionResource.session
                 .useKleisli(dbUpdater updateDB RollbackToTriplesGenerated(event.eventId.id, project))
                 .asserting(_ shouldBe DBUpdateResults(project.slug, TransformingTriples -> -1, TriplesGenerated -> 1))

          _ <- findEvent(event.eventId).asserting(_.map(_.status) shouldBe Some(TriplesGenerated))
          _ <- findEvent(otherEvent.eventId).asserting(_.map(_.status) shouldBe Some(TransformingTriples))
        } yield Succeeded
    }

    s"do nothing if event is not in the $TransformingTriples status" in testDBResource.use { implicit cfg =>
      val invalidStatus = Gen.oneOf(EventStatus.all.filterNot(_ == TransformingTriples)).generateOne
      val project       = consumerProjects.generateOne

      for {
        event <- addEvent(invalidStatus, project)

        _ <- moduleSessionResource.session
               .useKleisli(dbUpdater updateDB RollbackToTriplesGenerated(event.eventId.id, project))
               .asserting(_ shouldBe DBUpdateResults.empty)

        _ <- findEvent(event.eventId).asserting(_.map(_.status) shouldBe Some(invalidStatus))
      } yield Succeeded
    }
  }

  private implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private lazy val dbUpdater = new DbUpdater[IO](() => Instant.now())

  private def addEvent(status: EventStatus, project: Project)(implicit cfg: DBConfig[EventLogDB]): IO[GeneratedEvent] =
    storeGeneratedEvent(status, timestampsNotInTheFuture.generateAs(EventDate), project)
}
