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

import cats.data.Kleisli
import cats.effect.IO
import io.circe.literal._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.init.Generators._
import io.renku.eventlog.init.model.Event
import io.renku.eventlog.{CreatedDate, EventDate, ExecutionDate}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.{BatchDate, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.codec.all._
import skunk.implicits._

import java.time.{LocalDateTime, ZoneOffset}

class BatchDateAdderSpec extends AnyWordSpec with IOSpec with DbInitSpec with should.Matchers {

  private[this] implicit val logger: TestLogger[IO] = TestLogger[IO]()

  protected[init] override lazy val migrationsToRun: List[DbMigrator[IO]] = List(
    EventLogTableCreator[IO],
    ProjectPathAdder[IO]
  )

  "run" should {

    "do nothing if the 'event' table already exists" in new TestCase {

      createEventTable()

      batchDateAdder.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'batch_date' column adding skipped"))
    }

    "do nothing if the 'batch_date' column already exists" in new TestCase {

      checkColumnExists shouldBe false

      batchDateAdder.run().unsafeRunSync() shouldBe ()

      checkColumnExists shouldBe true

      logger.loggedOnly(Info("'batch_date' column added"))

      logger.reset()

      batchDateAdder.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'batch_date' column exists"))
    }

    "add the 'batch_date' column if does not exist and migrate the data for it" in new TestCase {

      checkColumnExists shouldBe false

      val event1            = events.generateOne
      val event1CreatedDate = createdDates.generateOne
      storeEvent(event1, event1CreatedDate)
      val event2            = events.generateOne
      val event2CreatedDate = createdDates.generateOne
      storeEvent(event2, event2CreatedDate)

      batchDateAdder.run().unsafeRunSync() shouldBe ()

      findBatchDates shouldBe Set(BatchDate(event1CreatedDate.value), BatchDate(event2CreatedDate.value))

      verifyTrue(sql"DROP INDEX idx_batch_date;".command)

      logger.loggedOnly(Info("'batch_date' column added"))
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val batchDateAdder = new BatchDateAdderImpl[IO]
  }

  private def checkColumnExists: Boolean =
    sessionResource
      .useK {
        Kleisli { session =>
          val query: Query[Void, BatchDate] = sql"select batch_date from event_log limit 1"
            .query(timestamp)
            .map { case time: LocalDateTime => BatchDate(time.toInstant(ZoneOffset.UTC)) }
          session
            .option(query)
            .map(_ => true)
            .recover { case _ => false }
        }
      }
      .unsafeRunSync()

  private def storeEvent(event: Event, createdDate: CreatedDate): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[
        EventId ~ projects.Id ~ projects.Path ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ String
      ] =
        sql"""insert into
              event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, event_body) 
              values (
                $eventIdEncoder, 
                $projectIdEncoder, 
                $projectPathEncoder, 
                $eventStatusEncoder, 
                $createdDateEncoder,
                $executionDateEncoder, 
                $eventDateEncoder, 
                $text
              )
      """.command
      session
        .prepare(query)
        .use(
          _.execute(
            event.id ~ event.project.id ~ event.project.path ~ eventStatuses.generateOne ~ createdDate ~ executionDates.generateOne ~ event.date ~ toJsonBody(
              event
            )
          )
        )
        .map(_ => ())
    }
  }

  private def toJsonBody(event: Event): String =
    json"""{
    "project": {
      "id": ${event.project.id.value},
      "path": ${event.project.path.value}
     }
  }""".noSpaces

  private def findBatchDates: Set[BatchDate] =
    sessionResource
      .useK {
        Kleisli { session =>
          val query: Query[Void, BatchDate] = sql"select batch_date from event_log"
            .query(timestamp)
            .map { case time: LocalDateTime => BatchDate(time.toInstant(ZoneOffset.UTC)) }

          session.execute(query)
        }
      }
      .unsafeRunSync()
      .toSet
}
