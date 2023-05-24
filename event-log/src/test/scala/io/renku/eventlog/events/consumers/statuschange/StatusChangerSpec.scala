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

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog._
import io.renku.eventlog.api.events.{StatusChangeEvent, StatusChangeGenerators}
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.api.events.StatusChangeEvent._
import io.renku.events.Generators.{subscriberIds, subscriberUrls}
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonNegativeInts
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.{EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.implicits._
import skunk.~

class StatusChangerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateStatuses" should {

    "succeeds if db update completes" in new MockedTestCase {

      val updateResults = updateResultsGen(event).generateOne

      (dbUpdater.updateDB _).expects(event).returning(Kleisli.pure(updateResults))
      (gaugesUpdater.updateGauges _).expects(updateResults).returning(().pure[IO])

      statusChanger.updateStatuses(dbUpdater)(event).unsafeRunSync() shouldBe ()
    }

    "rollback, run the updater's onRollback and fail if db update raises an error" in new NonMockedTestCase {

      findEvent(eventId).map(_._2) shouldBe Some(initialStatus)
      findAllEventDeliveries       shouldBe List(eventId -> subscriberId)

      val event: StatusChangeEvent = ToTriplesGenerated(eventId.id,
                                                        Project(eventId.projectId, projectPaths.generateOne),
                                                        eventProcessingTimes.generateOne,
                                                        zippedEventPayloads.generateOne
      )

      intercept[Exception] {
        statusChanger.updateStatuses(dbUpdater)(event).unsafeRunSync()
      }

      findEvent(eventId).map(_._2) shouldBe Some(initialStatus)
      findAllEventDeliveries       shouldBe Nil
    }

    "succeed if updating the gauge fails" in new MockedTestCase {

      val exception = Generators.exceptions.generateOne

      val updateResults = updateResultsGen(event).generateOne
      (dbUpdater.updateDB _).expects(event).returning(Kleisli.pure(updateResults))
      (gaugesUpdater.updateGauges _).expects(updateResults).returning(exception.raiseError[IO, Unit])

      statusChanger.updateStatuses(dbUpdater)(event).unsafeRunSync() shouldBe ()
    }
  }

  trait MockedTestCase {

    val event = Gen
      .oneOf(
        StatusChangeGenerators.toTriplesGeneratedEvents,
        StatusChangeGenerators.toTripleStoreEvents,
        StatusChangeGenerators.rollbackToNewEvents
      )
      .generateOne

    implicit val dbUpdater: DBUpdater[IO, StatusChangeEvent] = mock[DBUpdater[IO, StatusChangeEvent]]

    val gaugesUpdater = mock[GaugesUpdater[IO]]
    val statusChanger = new StatusChangerImpl[IO](gaugesUpdater)
  }

  trait NonMockedTestCase {

    val eventId               = compoundEventIds.generateOne
    val initialStatus         = EventStatus.New
    val subscriberId          = subscriberIds.generateOne
    private val subscriberUrl = subscriberUrls.generateOne
    private val sourceUrl     = microserviceBaseUrls.generateOne

    storeEvent(eventId, initialStatus, executionDates.generateOne, eventDates.generateOne, eventBodies.generateOne)
    upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)
    upsertEventDelivery(eventId, subscriberId)

    private class TestDbUpdater extends DbClient[IO](None) with DBUpdater[IO, StatusChangeEvent] {

      override def updateDB(event: StatusChangeEvent): UpdateOp[IO] = Kleisli { session =>
        val passingQuery = SqlStatement[IO](name = "passing dbUpdater query")
          .command[EventId](
            sql"""UPDATE event
                  SET status = '#${EventStatus.TriplesGenerated.value}'
                  WHERE event_id = $eventIdEncoder
           """.command
          )
          .arguments(eventId.id)
          .build
          .mapResult(_ => genUpdateResult(projectPaths.generateOne).generateOne)
          .queryExecution

        val failingQuery = SqlStatement[IO](name = "failing dbUpdater query")
          .command[EventId](
            sql"""UPDATE event
                  SET sta = '#${EventStatus.TriplesStore.value}'
                  WHERE event_id = $eventIdEncoder
           """.command
          )
          .arguments(eventId.id)
          .build
          .mapResult(_ => genUpdateResult(projectPaths.generateOne).generateOne)
          .queryExecution

        passingQuery.run(session) >> failingQuery.run(session)
      }

      override def onRollback(event: StatusChangeEvent): RollbackOp[IO] = Kleisli {
        SqlStatement[IO](name = "onRollback dbUpdater query")
          .command[EventId ~ projects.GitLabId](
            sql"""DELETE FROM event_delivery
                  WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
               """.command
          )
          .arguments(eventId.id ~ eventId.projectId)
          .build
          .void
          .queryExecution
          .run
      }
    }

    implicit val dbUpdater: DBUpdater[IO, StatusChangeEvent] = new TestDbUpdater
    val gaugesUpdater = mock[GaugesUpdater[IO]]
    val statusChanger = new StatusChangerImpl[IO](gaugesUpdater)
  }

  private def updateResultsGen(event: StatusChangeEvent): Gen[DBUpdateResults] = event match {
    case AllEventsToNew                       => Gen.const(DBUpdateResults.ForAllProjects)
    case ProjectEventsToNew(_)                => Gen.const(DBUpdateResults.ForProjects.empty)
    case RedoProjectTransformation(_)         => Gen.const(DBUpdateResults.ForProjects.empty)
    case ToTriplesGenerated(_, project, _, _) => genUpdateResult(project.path)
    case ToTriplesStore(_, project, _)        => genUpdateResult(project.path)
    case ev @ ToFailure(_, project, _, newStatus, _) =>
      Gen.const(
        DBUpdateResults.ForProjects(project.path, Map(ev.currentStatus -> -1, newStatus -> 1))
      )
    case RollbackToNew(_, project) =>
      Gen.const(
        DBUpdateResults.ForProjects(project.path, Map(EventStatus.GeneratingTriples -> -1, EventStatus.New -> 1))
      )
    case RollbackToTriplesGenerated(_, project) =>
      Gen.const(
        DBUpdateResults.ForProjects(project.path,
                                    Map(EventStatus.TransformingTriples -> -1, EventStatus.TriplesGenerated -> 1)
        )
      )
    case ToAwaitingDeletion(_, project) =>
      Gen.const(
        DBUpdateResults.ForProjects(project.path,
                                    Map(eventStatuses.generateOne -> -1, EventStatus.AwaitingDeletion -> 1)
        )
      )
    case RollbackToAwaitingDeletion(Project(_, projectPath)) =>
      val updatedRows = Generators.positiveInts(max = 40).generateOne
      Gen.const(
        DBUpdateResults.ForProjects(
          projectPath,
          Map(EventStatus.Deleting -> -updatedRows, EventStatus.AwaitingDeletion -> updatedRows)
        )
      )
  }

  private def genUpdateResult(forProject: projects.Path) = for {
    statuses <- eventStatuses.toGeneratorOfSet()
    counts   <- statuses.toList.map(s => nonNegativeInts().map(count => s -> count.value)).sequence
  } yield DBUpdateResults.ForProjects(forProject, counts.toMap)
}
