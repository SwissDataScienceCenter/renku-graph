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

import cats.Applicative
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.{DbClient, SqlStatement}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, nonNegativeInts}
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus.GeneratingTriples
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.projects
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.Generators._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.{AllEventsToNew, ToNew, ToTriplesGenerated, ToTriplesStore}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.implicits._

class StatusChangerSpec
    extends AnyWordSpec
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

    "rollback and fails if db update raises an error" in new NonMockedTestCase {

      findEvent(eventId).map(_._2) shouldBe Some(initialStatus)

      val event: StatusChangeEvent = ToTriplesGenerated(eventId,
                                                        projectPaths.generateOne,
                                                        eventProcessingTimes.generateOne,
                                                        eventPayloads.generateOne,
                                                        projectSchemaVersions.generateOne
      )

      intercept[Exception] {
        statusChanger.updateStatuses(event).unsafeRunSync()
      }

      findEvent(eventId).map(_._2) shouldBe Some(initialStatus)
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

    val event = Gen.oneOf(toTriplesGeneratedEvents, toTripleStoreEvents, toNewEvents).generateOne

    implicit val dbUpdater: DBUpdater[IO, StatusChangeEvent] = mock[DBUpdater[IO, StatusChangeEvent]]

    val gaugesUpdater = mock[GaugesUpdater[IO]]
    val statusChanger = new StatusChangerImpl[IO](sessionResource, gaugesUpdater)
  }

  private trait NonMockedTestCase {

    val eventId       = compoundEventIds.generateOne
    val initialStatus = EventStatus.New

    storeEvent(eventId, initialStatus, executionDates.generateOne, eventDates.generateOne, eventBodies.generateOne)

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
    }

    implicit val dbUpdater: DBUpdater[IO, StatusChangeEvent] = new TestDbUpdater
    val gaugesUpdater = mock[GaugesUpdater[IO]]
    val statusChanger = new StatusChangerImpl[IO](sessionResource, gaugesUpdater)
  }

  private implicit val genApplicative: Applicative[Gen] = new Applicative[Gen] {
    override def pure[A](x:   A) = Gen.const(x)
    override def ap[A, B](ff: Gen[A => B])(fa: Gen[A]): Gen[B] = ff.flatMap(f => fa.map(f))
  }

  private def updateResultsGen(event: StatusChangeEvent): Gen[DBUpdateResults] = event match {
    case AllEventsToNew                              => Gen.const(DBUpdateResults.ForAllProjects)
    case ToTriplesGenerated(_, projectPath, _, _, _) => genUpdateResult(projectPath)
    case ToTriplesStore(_, projectPath, _)           => genUpdateResult(projectPath)
    case ToNew(_, projectPath)                       => Gen.const(DBUpdateResults.ForProjects(projectPath, Map(GeneratingTriples -> 1)))
  }

  private def genUpdateResult(forProject: projects.Path) = for {
    statuses <- eventStatuses.toGeneratorOfSet()
    counts   <- statuses.toList.map(s => nonNegativeInts().map(count => s -> count.value)).sequence
  } yield DBUpdateResults.ForProjects(forProject, counts.toMap)
}
