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

package io.renku.eventlog.events.consumers.statuschange

import cats.effect.IO
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent.RedoProjectTransformation
import io.renku.eventlog.events.producers
import io.renku.eventlog.events.producers.SubscriptionDataProvisioning
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators.eventMessages
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.interpreters.TestLogger
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class RedoProjectTransformationPollerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "update the latest TRIPLES_STORE event of the given project to TRIPLES_GENERATED " +
      "when there are no events in TRIPLES_GENERATED already" in new TestCase {

        val project = consumerProjects.generateOne

        addEvent(TriplesStore, project)
          .addLaterEvent(GenerationNonRecoverableFailure)
          .addLaterEvent(TriplesStore)
          .addLaterEvent(New)

        findEventStatuses(project) shouldBe List(TriplesStore, GenerationNonRecoverableFailure, TriplesStore, New)

        val event = RedoProjectTransformation(project.path)
        sessionResource
          .useK(updater updateDB event)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(project.path,
                                                                Map(TriplesStore -> -1, TriplesGenerated -> 1)
        )

        findEventStatuses(project) shouldBe List(TriplesStore, GenerationNonRecoverableFailure, TriplesGenerated, New)

        sessionResource
          .useK(updater updateDB event)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty
      }

    "remove row from the subscription_category_sync_time for the given project and ADD_MIN_PROJECT_INFO " +
      "if there are no events in TRIPLES_STORE for it" in new TestCase {

        val project = consumerProjects.generateOne

        addEvent(Skipped, project)
          .addLaterEvent(GenerationNonRecoverableFailure)
          .addLaterEvent(New)

        findEventStatuses(project) shouldBe List(Skipped, GenerationNonRecoverableFailure, New)

        upsertCategorySyncTime(project.id,
                               producers.minprojectinfo.categoryName,
                               timestampsNotInTheFuture.generateAs(LastSyncedDate)
        )
        findSyncTime(project.id, producers.minprojectinfo.categoryName).isEmpty shouldBe false

        sessionResource
          .useK(updater updateDB RedoProjectTransformation(project.path))
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty

        findEventStatuses(project) shouldBe List(Skipped, GenerationNonRecoverableFailure, New)

        findSyncTime(project.id, producers.minprojectinfo.categoryName) shouldBe None
      }
  }

  private trait TestCase {
    implicit val logger:                   TestLogger[IO]            = TestLogger[IO]()
    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val updater = new RedoProjectTransformationPollerImpl[IO]
  }

  private type EventCreationResult = (CompoundEventId, Project, EventDate)

  private def addEvent(status:    EventStatus,
                       project:   Project = consumerProjects.generateOne,
                       eventDate: EventDate = timestampsNotInTheFuture.generateAs(EventDate)
  ): EventCreationResult = {
    val eventId = compoundEventIds.generateOne.copy(projectId = project.id)
    storeEvent(
      eventId,
      status,
      timestamps.generateAs(ExecutionDate),
      eventDate,
      eventBodies.generateOne,
      maybeMessage = status match {
        case _: FailureStatus => eventMessages.generateSome
        case _ => eventMessages.generateOption
      },
      maybeEventPayload = status match {
        case TriplesStore | TriplesGenerated => zippedEventPayloads.generateSome
        case AwaitingDeletion                => zippedEventPayloads.generateOption
        case _                               => zippedEventPayloads.generateNone
      },
      projectPath = project.path
    )
    (eventId, project, eventDate)
  }

  private implicit class EventCreationResultOps(result: EventCreationResult) {
    private val (_, project, lastEventDate) = result

    def addLaterEvent(status: EventStatus): EventCreationResult =
      addEvent(status, project, timestampsNotInTheFuture(butYoungerThan = lastEventDate.value).generateAs(EventDate))
  }

  private def findEventStatuses(project: Project) =
    findAllProjectEvents(project.id).flatMap(findEvent).map(_._2)
}
