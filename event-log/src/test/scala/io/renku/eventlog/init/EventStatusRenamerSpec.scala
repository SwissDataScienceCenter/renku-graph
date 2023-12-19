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

package io.renku.eventlog.init

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.circe.literal.JsonStringContext
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.init.model.Event
import io.renku.eventlog.{events => _, _}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.codec.all._
import skunk.implicits._

class EventStatusRenamerSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[EventStatusRenamer[IO]]

  it should "rename all the events from PROCESSING to GENERATING_TRIPLES, " +
    "RECOVERABLE_FAILURE to GENERATION_RECOVERABLE_FAILURE and " +
    "NON_RECOVERABLE_FAILURE to GENERATION_NON_RECOVERABLE_FAILURE" in testDBResource.use { implicit cfg =>
      val processingEvents = events.generateNonEmptyList(min = 2)
      for {
        _ <- processingEvents.traverse_(store(_, withStatus = "PROCESSING"))

        recoverableEvents = events.generateNonEmptyList(min = 2)
        _ <- recoverableEvents.traverse_(store(_, withStatus = "RECOVERABLE_FAILURE"))

        nonRecoverableEvents = events.generateNonEmptyList(min = 2)
        _ <- nonRecoverableEvents.traverse_(store(_, withStatus = "NON_RECOVERABLE_FAILURE"))

        otherEvents = events.generateNonEmptyList()
        _ <- otherEvents.traverse_(event => store(event, withStatus = event.status.toString))

        _ <- eventStatusRenamer.run.assertNoException

        _ <- findEventIds(status = GeneratingTriples).asserting(
               _.toSet shouldBe
                 (processingEvents.toList ++ otherEvents.filter(_.status == GeneratingTriples))
                   .map(_.compoundEventId)
                   .toSet
             )
        _ <- findEventIds(status = GenerationRecoverableFailure).asserting(
               _.toSet shouldBe
                 (recoverableEvents ++ otherEvents.filter(_.status == GenerationRecoverableFailure))
                   .map(_.compoundEventId)
                   .toList
                   .toSet
             )
        _ <- findEventIds(status = GenerationNonRecoverableFailure).asserting(
               _.toSet shouldBe
                 (nonRecoverableEvents ++ otherEvents.filter(_.status == GenerationNonRecoverableFailure))
                   .map(_.compoundEventId)
                   .toList
                   .toSet
             )

        _ <- logger.loggedOnlyF(
               Info(s"'PROCESSING' event status renamed to 'GENERATING_TRIPLES'"),
               Info(s"'RECOVERABLE_FAILURE' event status renamed to 'GENERATION_RECOVERABLE_FAILURE'"),
               Info(s"'NON_RECOVERABLE_FAILURE' event status renamed to 'GENERATION_NON_RECOVERABLE_FAILURE'")
             )
      } yield Succeeded
    }

  it should "do nothing if there are no events with the status PROCESSING" in testDBResource.use { implicit cfg =>
    val otherEvents = events.generateNonEmptyList()
    for {
      _ <- otherEvents.traverse_(event => store(event, withStatus = event.status.show))

      _ <- eventStatusRenamer.run.assertNoException

      _ <- findEventIds(status = GeneratingTriples).asserting(
             _.toSet shouldBe otherEvents
               .filter(_.status == GeneratingTriples)
               .map(_.compoundEventId)
               .toSet
           )

      _ <- findEventsId.asserting(_ shouldBe otherEvents.map(_.id).toList.toSet)
    } yield Succeeded
  }

  private def eventStatusRenamer(implicit cfg: DBConfig[EventLogDB]) = new EventStatusRenamerImpl[IO]

  private def store(event: Event, withStatus: String)(implicit cfg: DBConfig[EventLogDB]) =
    upsertProject(event.compoundEventId.projectId, event.project.slug, event.date) >>
      moduleSessionResource.session.use { session =>
        val query: Command[
          EventId *: projects.GitLabId *: String *: CreatedDate *: ExecutionDate *: EventDate *: String *: BatchDate *: EmptyTuple
        ] =
          sql"""INSERT INTO event (event_id, project_id, status, created_date, execution_date, event_date, event_body, batch_date)
                VALUES (
                  $eventIdEncoder,
                  $projectIdEncoder,
                  $varchar,
                  $createdDateEncoder,
                  $executionDateEncoder,
                  $eventDateEncoder,
                  $text,
                  $batchDateEncoder
                )
                """.command
        session
          .prepare(query)
          .flatMap(
            _.execute(
              event.id *:
                event.project.id *:
                withStatus *:
                createdDates.generateOne *:
                executionDates.generateOne *:
                event.date *:
                toJsonBody(event) *:
                event.batchDate *:
                EmptyTuple
            )
          )
          .void
      }

  private def upsertProject(projectId: projects.GitLabId, projectSlug: projects.Slug, eventDate: EventDate)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Unit] =
    moduleSessionResource.session.use { session =>
      val query: Command[projects.GitLabId *: projects.Slug *: EventDate *: EmptyTuple] =
        sql"""INSERT INTO project (project_id, project_path, latest_event_date)
              VALUES ($projectIdEncoder, $projectSlugEncoder, $eventDateEncoder)
              ON CONFLICT (project_id)
              DO UPDATE SET latest_event_date = excluded.latest_event_date WHERE excluded.latest_event_date > project.latest_event_date
          """.command
      session.prepare(query).flatMap(_.execute(projectId *: projectSlug *: eventDate *: EmptyTuple)).void
    }

  private def toJsonBody(event: Event): String = json"""{
    "project": {
      "id":   ${event.project.id},
      "slug": ${event.project.slug}
     }
  }""".noSpaces

  private def findEventIds(status: EventStatus)(implicit cfg: DBConfig[EventLogDB]): IO[List[CompoundEventId]] =
    moduleSessionResource.session.use { session =>
      val query: Query[EventStatus, CompoundEventId] =
        sql"""SELECT event_id, project_id
              FROM event
              WHERE status = $eventStatusEncoder
              ORDER BY created_date asc"""
          .query(eventIdDecoder ~ projectIdDecoder)
          .map { case eventId ~ projectId => CompoundEventId(eventId, projectId) }
      session.prepare(query).flatMap(_.stream(status, 32).compile.toList)
    }

  private def findEventsId(implicit cfg: DBConfig[EventLogDB]): IO[Set[EventId]] =
    moduleSessionResource.session
      .use { session =>
        val query: Query[Void, EventId] = sql"SELECT event_id FROM event".query(eventIdDecoder)
        session.execute(query)
      }
      .map(_.toSet)
}
