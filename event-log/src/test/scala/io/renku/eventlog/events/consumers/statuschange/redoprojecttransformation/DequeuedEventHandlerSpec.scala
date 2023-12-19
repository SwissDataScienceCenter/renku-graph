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

package io.renku.eventlog.events.consumers.statuschange.redoprojecttransformation

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent.{ProjectSlug, RedoProjectTransformation}
import io.renku.eventlog.events.consumers.statuschange.DBUpdateResults
import io.renku.eventlog.events.producers
import io.renku.eventlog.events.producers.SubscriptionProvisioning
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class DequeuedEventHandlerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with should.Matchers {

  "updateDB" should {

    "update the latest TRIPLES_STORE event of the given project to TRIPLES_GENERATED " +
      "when there are no events in TRIPLES_GENERATED already" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne

        for {
          _ <- addEvent(TriplesStore, project)
                 .addLaterEvent(GenerationNonRecoverableFailure)
                 .addLaterEvent(TriplesStore)
                 .addLaterEvent(New)

          _ <- findEventStatuses(project).asserting(
                 _ shouldBe List(TriplesStore, GenerationNonRecoverableFailure, TriplesStore, New)
               )

          event = RedoProjectTransformation(ProjectSlug(project.slug))
          _ <- moduleSessionResource.session
                 .useKleisli(updater updateDB event)
                 .asserting(_ shouldBe DBUpdateResults(project.slug, TriplesStore -> -1, TriplesGenerated -> 1))

          _ <- findEventStatuses(project)
                 .asserting(_ shouldBe List(TriplesStore, GenerationNonRecoverableFailure, TriplesGenerated, New))

          _ <- moduleSessionResource.session
                 .useKleisli(updater updateDB event)
                 .asserting(_ shouldBe DBUpdateResults.empty)
        } yield Succeeded
      }

    "remove row from the subscription_category_sync_time for the given project and ADD_MIN_PROJECT_INFO " +
      "if there are no events in TRIPLES_STORE for it" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne

        for {
          _ <- addEvent(Skipped, project)
                 .addLaterEvent(GenerationNonRecoverableFailure)
                 .addLaterEvent(New)

          _ <- findEventStatuses(project).asserting(_ shouldBe List(Skipped, GenerationNonRecoverableFailure, New))

          _ <- upsertCategorySyncTime(project.id, producers.minprojectinfo.categoryName)
          _ <- findSyncTime(project.id, producers.minprojectinfo.categoryName).asserting(_ shouldBe a[Some[_]])

          _ <- moduleSessionResource.session
                 .useKleisli(updater updateDB RedoProjectTransformation(ProjectSlug(project.slug)))
                 .asserting(_ shouldBe DBUpdateResults.empty)

          _ <- findEventStatuses(project).asserting(_ shouldBe List(Skipped, GenerationNonRecoverableFailure, New))

          _ <- findSyncTime(project.id, producers.minprojectinfo.categoryName).asserting(_ shouldBe None)
        } yield Succeeded
      }
  }

  private lazy val updater = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DequeuedEventHandlerImpl[IO]
  }

  private type EventCreationResult = (CompoundEventId, Project, EventDate)

  private def addEvent(status:    EventStatus,
                       project:   Project = consumerProjects.generateOne,
                       eventDate: EventDate = timestampsNotInTheFuture.generateAs(EventDate)
  )(implicit cfg: DBConfig[EventLogDB]): IO[EventCreationResult] =
    storeGeneratedEvent(status, eventDate, project).map(ge => (ge.eventId, project, eventDate))

  private implicit class EventCreationResultOps(result: IO[EventCreationResult]) {

    def addLaterEvent(status: EventStatus)(implicit cfg: DBConfig[EventLogDB]): IO[EventCreationResult] =
      result >>= { case (_, project, lastEventDate) =>
        addEvent(status, project, timestampsNotInTheFuture(butYoungerThan = lastEventDate.value).generateAs(EventDate))
      }
  }

  private def findEventStatuses(project: Project)(implicit cfg: DBConfig[EventLogDB]) =
    findAllProjectEvents(project.id)
      .flatMap(_.map(findEvent).sequence)
      .map(_.flatten.map(_.status))
}
