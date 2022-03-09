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

package io.renku.eventlog.init

import Generators._
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.literal.JsonStringContext
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.init.model.Event
import io.renku.eventlog.{events => _, _}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{BatchDate, CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.codec.all._
import skunk.implicits._

class EventStatusRenamerSpec
    extends AnyWordSpec
    with IOSpec
    with DbInitSpec
    with should.Matchers
    with EventLogDataProvisioning
    with EventDataFetching {

  protected[init] override lazy val migrationsToRun: List[DbMigrator[IO]] = allMigrations.takeWhile {
    case _: EventStatusRenamerImpl[IO] => false
    case _ => true
  }

  "run" should {
    "rename all the events from PROCESSING to GENERATING_TRIPLES, " +
      "RECOVERABLE_FAILURE to GENERATION_RECOVERABLE_FAILURE and " +
      "NON_RECOVERABLE_FAILURE to GENERATION_NON_RECOVERABLE_FAILURE" in new TestCase {
        val processingEvents = events.generateNonEmptyList(minElements = 2)
        processingEvents.map(event => store(event, withStatus = "PROCESSING"))

        val recoverableEvents = events.generateNonEmptyList(minElements = 2)
        recoverableEvents.map(event => store(event, withStatus = "RECOVERABLE_FAILURE"))

        val nonRecoverableEvents = events.generateNonEmptyList(minElements = 2)
        nonRecoverableEvents.map(event => store(event, withStatus = "NON_RECOVERABLE_FAILURE"))

        val otherEvents = events.generateNonEmptyList()
        otherEvents.map(event => store(event, withStatus = event.status.toString))

        eventStatusRenamer.run().unsafeRunSync() shouldBe ()

        findEventsCompoundId(status = GeneratingTriples).toSet shouldBe
          (processingEvents.toList ++ otherEvents.filter(_.status == GeneratingTriples))
            .map(_.compoundEventId)
            .toSet
        findEventsCompoundId(status = GenerationRecoverableFailure).toSet shouldBe
          (recoverableEvents ++ otherEvents.filter(_.status == GenerationRecoverableFailure))
            .map(_.compoundEventId)
            .toList
            .toSet
        findEventsCompoundId(status = GenerationNonRecoverableFailure).toSet shouldBe
          (nonRecoverableEvents ++ otherEvents.filter(_.status == GenerationNonRecoverableFailure))
            .map(_.compoundEventId)
            .toList
            .toSet

        logger.loggedOnly(
          Info(s"'PROCESSING' event status renamed to 'GENERATING_TRIPLES'"),
          Info(s"'RECOVERABLE_FAILURE' event status renamed to 'GENERATION_RECOVERABLE_FAILURE'"),
          Info(s"'NON_RECOVERABLE_FAILURE' event status renamed to 'GENERATION_NON_RECOVERABLE_FAILURE'")
        )
      }

    "do nothing if there are no events with the status PROCESSING" in new TestCase {
      val otherEvents = events.generateNonEmptyList()
      otherEvents.map(event => store(event, withStatus = event.status.show))

      eventStatusRenamer.run().unsafeRunSync() shouldBe ()

      findEventsCompoundId(status = GeneratingTriples).toSet shouldBe otherEvents
        .filter(_.status == GeneratingTriples)
        .map(_.compoundEventId)
        .toSet

      findEventsId shouldBe otherEvents.map(_.id).toList.toSet
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventStatusRenamer = new EventStatusRenamerImpl[IO]
  }

  private def store(event: Event, withStatus: String): Unit = {
    upsertProject(event.compoundEventId, event.project.path, event.date)
    execute[Unit] {
      Kleisli { session =>
        val query
            : Command[EventId ~ projects.Id ~ String ~ CreatedDate ~ ExecutionDate ~ EventDate ~ String ~ BatchDate] =
          sql"""INSERT INTO 
              event (event_id, project_id, status, created_date, execution_date, event_date, event_body, batch_date) 
              values (
              $eventIdEncoder, 
              $projectIdEncoder, 
              $varchar, 
              $createdDateEncoder,
              $executionDateEncoder, 
              $eventDateEncoder, 
              $text,
              $batchDateEncoder)
      """.command
        session
          .prepare(query)
          .use(
            _.execute(
              event.id ~ event.project.id ~ withStatus ~ createdDates.generateOne ~ executionDates.generateOne ~ event.date ~ toJsonBody(
                event
              ) ~ event.batchDate
            )
          )
          .void
      }
    }
  }

  private def toJsonBody(event: Event): String =
    json"""{
    "project": {
      "id": ${event.project.id.value},
      "path": ${event.project.path.value}
     }
  }""".noSpaces

  private def findEventsId: Set[EventId] = sessionResource
    .useK {
      Kleisli { session =>
        val query: Query[Void, EventId] = sql"SELECT event_id FROM event".query(eventIdDecoder)
        session.execute(query)
      }
    }
    .unsafeRunSync()
    .toSet

  private def findEventsCompoundId(status: EventStatus): List[CompoundEventId] = execute[List[CompoundEventId]] {
    Kleisli { session =>
      val query: Query[EventStatus, CompoundEventId] =
        sql"""SELECT event_id, project_id
                FROM event
                WHERE status = $eventStatusEncoder
                ORDER BY created_date asc"""
          .query(eventIdDecoder ~ projectIdDecoder)
          .map { case eventId ~ projectId => CompoundEventId(eventId, projectId) }
      session.prepare(query).use(_.stream(status, 32).compile.toList)
    }
  }
}
