/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.literal._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.init.Generators._
import io.renku.eventlog.init.model.Event
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.codec.all._
import skunk.implicits._

import java.time.{LocalDateTime, ZoneOffset}

class BatchDateAdderSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[BatchDateAdder[IO]]

  it should "do nothing if the 'event' table already exists" in testDBResource.use { implicit cfg =>
    createEventTable >>
      logger.resetF() >>
      batchDateAdder.run.assertNoException >>
      logger.loggedOnlyF(Info("'batch_date' column adding skipped"))
  }

  it should "do nothing if the 'batch_date' column already exists" in testDBResource.use { implicit cfg =>
    for {
      _ <- verifyColumnExists("event_log", "batch_date").asserting(_ shouldBe false)

      _ <- batchDateAdder.run.assertNoException

      _ <- verifyColumnExists("event_log", "batch_date").asserting(_ shouldBe true)

      _ <- logger.loggedOnlyF(Info("'batch_date' column added"))

      _ <- logger.resetF()

      _ <- batchDateAdder.run.assertNoException

      _ <- logger.loggedOnlyF(Info("'batch_date' column exists"))
    } yield Succeeded
  }

  it should "add the 'batch_date' column if does not exist and migrate the data for it" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- verifyColumnExists("event_log", "batch_date").asserting(_ shouldBe false)

        event1            = events.generateOne
        event1CreatedDate = createdDates.generateOne
        _ <- storeEvent(event1, event1CreatedDate)
        event2            = events.generateOne
        event2CreatedDate = createdDates.generateOne
        _ <- storeEvent(event2, event2CreatedDate)

        _ <- batchDateAdder.run.assertNoException

        _ <- findBatchDates.asserting(
               _ shouldBe Set(BatchDate(event1CreatedDate.value), BatchDate(event2CreatedDate.value))
             )

        _ <- verifyIndexExists("event_log", "idx_batch_date").asserting(_ shouldBe true)

        _ <- logger.loggedOnlyF(Info("'batch_date' column added"))
      } yield Succeeded
  }

  private def batchDateAdder(implicit cfg: DBConfig[EventLogDB]) = new BatchDateAdderImpl[IO]

  private def storeEvent(event: Event, createdDate: CreatedDate)(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource.session.use { session =>
      val query: Command[
        EventId *: projects.GitLabId *: projects.Slug *: EventStatus *: CreatedDate *: ExecutionDate *: EventDate *: String *: EmptyTuple
      ] = sql"""
         INSERT INTO event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, event_body)
         VALUES (
                $eventIdEncoder, 
                $projectIdEncoder, 
                $projectSlugEncoder,
                $eventStatusEncoder, 
                $createdDateEncoder,
                $executionDateEncoder, 
                $eventDateEncoder, 
                $text
              )
      """.command
      session
        .prepare(query)
        .flatMap(
          _.execute(
            event.id *:
              event.project.id *:
              event.project.slug *:
              eventStatuses.generateOne *:
              createdDate *:
              executionDates.generateOne *:
              event.date *:
              toJsonBody(event) *:
              EmptyTuple
          )
        )
        .void
    }

  private def toJsonBody(event: Event): String =
    json"""{
      "project": {
        "id":   ${event.project.id},
        "slug": ${event.project.slug}
       }
    }""".noSpaces

  private def findBatchDates(implicit cfg: DBConfig[EventLogDB]): IO[Set[BatchDate]] =
    moduleSessionResource.session
      .use { session =>
        val query: Query[Void, BatchDate] = sql"select batch_date from event_log"
          .query(timestamp)
          .map { case time: LocalDateTime => BatchDate(time.toInstant(ZoneOffset.UTC)) }
        session.execute(query)
      }
      .map(_.toSet)
}
