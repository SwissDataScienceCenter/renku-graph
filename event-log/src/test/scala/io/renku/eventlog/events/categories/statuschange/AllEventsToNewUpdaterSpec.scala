/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.categories.statuschange

import cats.effect.IO
import ch.datascience.db.SqlStatement
import ch.datascience.events.consumers.subscriptions.{subscriberIds, subscriberUrls}
import ch.datascience.generators.CommonGraphGenerators.microserviceBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{timestamps, timestampsNotInTheFuture}
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventProcessingTimes}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.{eventMessages, eventPayloads}
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.AllEventsToNew
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.util.Random

class AllEventsToNewUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change the status of all events to NEW except SKIPPED events" in new TestCase {

      val eventIds = projectIdentifiers
        .generateNonEmptyList(2)
        .flatMap { case (projectId, projectPath) =>
          Gen
            .oneOf(EventStatus.all.diff(Set(EventStatus.Skipped, EventStatus.AwaitingDeletion)))
            .generateNonEmptyList(2)
            .map(addEvent(_, projectId, projectPath))
            .map(CompoundEventId(_, projectId))
        }
        .toList

      val skippedEventProjectId = projectIds.generateOne
      val skippedEvent          = addEvent(EventStatus.Skipped, skippedEventProjectId)

      val awaitingDeletionEventProjectId = projectIds.generateOne
      val awaitingDeletionEvent          = addEvent(EventStatus.AwaitingDeletion, skippedEventProjectId)

      eventIds.foreach(upsertEventDelivery(_, subscriberId))

      sessionResource
        .useK(dbUpdater.updateDB(AllEventsToNew))
        .unsafeRunSync() shouldBe DBUpdateResults.ForAllProjects

      eventIds.flatMap(findFullEvent) shouldBe eventIds.map { eventId =>
        (eventId.id, EventStatus.New, None, None, List())
      }

      findEvent(CompoundEventId(skippedEvent, skippedEventProjectId)).map(_._2)                   shouldBe Some(EventStatus.Skipped)
      findEvent(CompoundEventId(awaitingDeletionEvent, awaitingDeletionEventProjectId)).map(_._2) shouldBe None
      findAllDeliveries                                                                           shouldBe Nil
    }
  }

  private trait TestCase {

    val subscriberId          = subscriberIds.generateOne
    private val subscriberUrl = subscriberUrls.generateOne
    private val sourceUrl     = microserviceBaseUrls.generateOne
    upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new AllEventsToNewUpdater[IO](queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now)

    def addEvent(status:      EventStatus,
                 projectId:   projects.Id = projectIds.generateOne,
                 projectPath: projects.Path = projectPaths.generateOne
    ): EventId = {
      val eventId = compoundEventIds.generateOne.copy(projectId = projectId)
      storeEvent(
        eventId,
        status,
        timestamps.generateAs(ExecutionDate),
        timestampsNotInTheFuture.generateAs(EventDate),
        eventBodies.generateOne,
        maybeMessage = status match {
          case _: EventStatus.FailureStatus => eventMessages.generateSome
          case _ => eventMessages.generateOption
        },
        maybeEventPayload = status match {
          case EventStatus.TriplesStore | EventStatus.TriplesGenerated => eventPayloads.generateSome
          case EventStatus.AwaitingDeletion                            => eventPayloads.generateOption
          case _                                                       => eventPayloads.generateNone
        },
        projectPath = projectPath
      )

      status match {
        case EventStatus.TriplesGenerated | EventStatus.TriplesStore =>
          upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
        case EventStatus.AwaitingDeletion =>
          if (Random.nextBoolean()) {
            upsertProcessingTime(eventId, status, eventProcessingTimes.generateOne)
          } else ()
        case _ => ()
      }

      eventId.id
    }

    def findFullEvent(eventId: CompoundEventId) = {
      val maybeEvent     = findEvent(eventId)
      val maybePayload   = findPayload(eventId).map(_._2)
      val processingTime = findProcessingTime(eventId)
      maybeEvent.map { case (_, status, maybeMessage) =>
        (eventId.id, status, maybeMessage, maybePayload, processingTime.map(_._2))
      }
    }

    lazy val projectIdentifiers = for {
      id   <- projectIds
      path <- projectPaths
    } yield (id, path)

  }
}
