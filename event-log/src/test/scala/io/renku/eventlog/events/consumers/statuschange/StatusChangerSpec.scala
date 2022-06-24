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

package io.renku.eventlog.events.consumers.statuschange

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.events.consumers.statuschange.Generators._
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent._
import io.renku.events.consumers.subscriptions.{subscriberIds, subscriberUrls}
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonNegativeInts, positiveInts}
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.implicits._
import skunk.{Session, ~}
import io.renku.events.consumers.Project

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

      statusChanger.updateStatuses(event).unsafeRunSync() shouldBe ()
    }

    "rollback, run the updater's onRollback and fail if db update raises an error" in new NonMockedTestCase {

      findEvent(eventId).map(_._2) shouldBe Some(initialStatus)
      findAllEventDeliveries       shouldBe List(eventId -> subscriberId)

      val event: StatusChangeEvent = ToTriplesGenerated(eventId,
                                                        projectPaths.generateOne,
                                                        eventProcessingTimes.generateOne,
                                                        zippedEventPayloads.generateOne
      )

      intercept[Exception] {
        statusChanger.updateStatuses(event).unsafeRunSync()
      }

      findEvent(eventId).map(_._2) shouldBe Some(initialStatus)
      findAllEventDeliveries       shouldBe Nil
    }

    "succeed if updating the gauge fails" in new MockedTestCase {

      val exception = exceptions.generateOne

      val updateResults = updateResultsGen(event).generateOne
      (dbUpdater.updateDB _).expects(event).returning(Kleisli.pure(updateResults))
      (gaugesUpdater.updateGauges _).expects(updateResults).returning(exception.raiseError[IO, Unit])

      statusChanger.updateStatuses(event).unsafeRunSync() shouldBe ()
    }
  }

  private trait MockedTestCase {

    val event = Gen.oneOf(toTriplesGeneratedEvents, toTripleStoreEvents, rollbackToNewEvents).generateOne

    implicit val dbUpdater: DBUpdater[IO, StatusChangeEvent] = mock[DBUpdater[IO, StatusChangeEvent]]

    val gaugesUpdater = mock[GaugesUpdater[IO]]
    val statusChanger = new StatusChangerImpl[IO](gaugesUpdater)
  }

  private trait NonMockedTestCase {

    val eventId               = compoundEventIds.generateOne
    val initialStatus         = EventStatus.New
    val subscriberId          = subscriberIds.generateOne
    private val subscriberUrl = subscriberUrls.generateOne
    private val sourceUrl     = microserviceBaseUrls.generateOne

    storeEvent(eventId, initialStatus, executionDates.generateOne, eventDates.generateOne, eventBodies.generateOne)
    upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)
    upsertEventDelivery(eventId, subscriberId)

    private class TestDbUpdater extends DbClient[IO](None) with DBUpdater[IO, StatusChangeEvent] {

      override def updateDB(event: StatusChangeEvent): UpdateResult[IO] = Kleisli { session =>
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

      override def onRollback(event: StatusChangeEvent): Kleisli[IO, Session[IO], Unit] = Kleisli {
        SqlStatement[IO](name = "onRollback dbUpdater query")
          .command[EventId ~ projects.Id](
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
    case AllEventsToNew                           => Gen.const(DBUpdateResults.ForAllProjects)
    case ProjectEventsToNew(project)              => genUpdateResult(project.path)
    case ToTriplesGenerated(_, projectPath, _, _) => genUpdateResult(projectPath)
    case ToTriplesStore(_, projectPath, _)        => genUpdateResult(projectPath)
    case ToFailure(_, projectPath, _, currentStatus, newStatus, _) =>
      Gen.const(
        DBUpdateResults.ForProjects(projectPath, Map(currentStatus -> -1, newStatus -> 1))
      )
    case RollbackToNew(_, projectPath) =>
      Gen.const(DBUpdateResults.ForProjects(projectPath, Map(GeneratingTriples -> -1, New -> 1)))
    case RollbackToTriplesGenerated(_, projectPath) =>
      Gen.const(DBUpdateResults.ForProjects(projectPath, Map(TransformingTriples -> -1, TriplesGenerated -> 1)))
    case ToAwaitingDeletion(_, projectPath) =>
      Gen.const(DBUpdateResults.ForProjects(projectPath, Map(eventStatuses.generateOne -> -1, AwaitingDeletion -> 1)))
    case RollbackToAwaitingDeletion(Project(_, projectPath)) =>
      val updatedRows = positiveInts(max = 40).generateOne
      Gen.const(
        DBUpdateResults.ForProjects(projectPath, Map(Deleting -> -updatedRows, AwaitingDeletion -> updatedRows))
      )
  }

  private def genUpdateResult(forProject: projects.Path) = for {
    statuses <- eventStatuses.toGeneratorOfSet()
    counts   <- statuses.toList.map(s => nonNegativeInts().map(count => s -> count.value)).sequence
  } yield DBUpdateResults.ForProjects(forProject, counts.toMap)
}
