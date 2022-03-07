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

package io.renku.eventlog.events.categories.statuschange

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages}
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.{AllEventsToNew, ProjectEventsToNew}
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.{EventId, EventStatus}
import io.renku.interpreters.TestLogger
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class AllEventsToNewUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "send ProjectEventsToNew events for all the projects" in new TestCase {

      val projects = projectObjects.generateNonEmptyList().toList

      projects foreach { project =>
        if (Random.nextBoolean()) addEvent(project)
        else upsertProject(project.id, project.path, eventDates.generateOne)
      }

      projects foreach { project =>
        (eventSender
          .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
          .expects(
            EventRequestContent.NoPayload(toEventJson(project)),
            EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                                     show"$categoryName: Generating ${ProjectEventsToNew.eventType} for $project failed"
            )
          )
          .returning(().pure[IO])
      }

      sessionResource.useK(dbUpdater updateDB AllEventsToNew).unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty
    }

    "do not send any events if there are no projects in the DB" in new TestCase {
      sessionResource.useK(dbUpdater updateDB AllEventsToNew).unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty
    }
  }

  private trait TestCase {

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventSender      = mock[EventSender[IO]]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new AllEventsToNewUpdater[IO](eventSender, queriesExecTimes)

    def addEvent(project: Project): EventId = {
      val eventId = compoundEventIds.generateOne.copy(projectId = project.id)
      val status  = eventStatuses.generateOne
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
          case EventStatus.TriplesStore | EventStatus.TriplesGenerated => zippedEventPayloads.generateSome
          case EventStatus.AwaitingDeletion                            => zippedEventPayloads.generateOption
          case _                                                       => zippedEventPayloads.generateNone
        },
        projectPath = project.path
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
  }

  private lazy val projectObjects: Gen[Project] = (projectIds -> projectPaths).mapN(Project.apply)

  private def toEventJson(project: Project) = json"""{
    "categoryName": "EVENTS_STATUS_CHANGE",
    "project": {
      "id":   ${project.id.value},
      "path": ${project.path.value}
    },
    "newStatus": ${EventStatus.New.value}
  }"""
}
