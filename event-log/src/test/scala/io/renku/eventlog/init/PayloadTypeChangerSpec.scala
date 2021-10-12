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

package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies, eventStatuses}
import ch.datascience.graph.model.GraphModelGenerators.{projectPaths, projectSchemaVersions}
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.{CreatedDate, EventDate, ExecutionDate}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.codec.all._
import skunk.implicits._
import skunk.{Command, Query, ~}

class PayloadTypeChangerSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[Migration] =
    allMigrations.takeWhile {
      case _: PayloadTypeChangerImpl[_] => false
      case _ => true
    }

  "run" should {

    "remove the schema_version and change the type of the payload column to bytea" in new TestCase {

      tableExists("event_payload") shouldBe true

      verify("event_payload", "payload", "text")
      verify("event_payload", "schema_version", "text")

      generateEvent(compoundEventIds.generateOne)

      tableRefactor.run().unsafeRunSync() shouldBe ()

      verify("event_payload", "payload", "bytea")
      verifyColumnExists("event_payload", "schema_version") shouldBe false
      execute[Long] {
        Kleisli { session =>
          val query: Query[skunk.Void, Long] = sql"""SELECT COUNT(*) FROM event_payload""".query(int8)
          session.unique(query)
        }
      } shouldBe 0L

    }

    "do nothing if the payload is already a bytea" in new TestCase {

      tableExists("event_payload") shouldBe true

      verify("event_payload", "payload", "text")
      verify("event_payload", "schema_version", "text")

      tableRefactor.run().unsafeRunSync() shouldBe ()

      verify("event_payload", "payload", "bytea")
      verifyColumnExists("event_payload", "schema_version") shouldBe false

      tableRefactor.run().unsafeRunSync() shouldBe ()

      verify("event_payload", "payload", "bytea")
      verifyColumnExists("event_payload", "schema_version") shouldBe false

      logger.loggedOnly(Info("event_payload.payload already in bytea type"))
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tableRefactor = new PayloadTypeChangerImpl[IO](sessionResource)
  }

  private def generateEvent(eventId: CompoundEventId): Unit = {
    upsertProject(eventId.projectId)
    insertEvent(eventId)
    insertPayload(eventId)
  }

  private def insertEvent(eventId: CompoundEventId) = execute[Unit] {
    Kleisli { session =>
      val query: Command[
        EventId ~ projects.Id ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ EventBody ~ BatchDate
      ] =
        sql"""INSERT INTO 
              event (event_id, project_id, status, created_date, execution_date, event_date, event_body, batch_date) 
              values (
              $eventIdEncoder, 
              $projectIdEncoder, 
              $eventStatusEncoder, 
              $createdDateEncoder,
              $executionDateEncoder, 
              $eventDateEncoder, 
              $eventBodyEncoder,
              $batchDateEncoder)
      """.command
      session
        .prepare(query)
        .use(
          _.execute(
            eventId.id ~ eventId.projectId ~ eventStatuses.generateOne ~ createdDates.generateOne ~
              executionDates.generateOne ~ eventDates.generateOne ~ eventBodies.generateOne ~ batchDates.generateOne
          )
        )
        .void
    }
  }

  private def insertPayload(eventId: CompoundEventId) = execute[Unit] {
    Kleisli { session =>
      val query: Command[EventId ~ projects.Id ~ String ~ String] =
        sql"""INSERT INTO 
              event_payload (event_id, project_id, payload, schema_version) 
              values ($eventIdEncoder,$projectIdEncoder, $text, $text)""".command
      session
        .prepare(query)
        .use(
          _.execute(
            eventId.id ~ eventId.projectId ~ nonEmptyStrings().generateOne ~ projectSchemaVersions.generateOne.value
          )
        )
        .void
    }
  }

  private def upsertProject(projectId: projects.Id): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[projects.Id ~ projects.Path ~ EventDate] =
        sql"""INSERT INTO
                project (project_id, project_path, latest_event_date)
                VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
          """.command
      session.prepare(query).use(_.execute(projectId ~ projectPaths.generateOne ~ eventDates.generateOne)).void
    }
  }

}
