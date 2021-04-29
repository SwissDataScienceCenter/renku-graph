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
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Path
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import io.circe.literal._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.{CreatedDate, Event, EventDate, ExecutionDate}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._
import skunk.codec.all._

class ProjectPathAdderSpec
    extends AnyWordSpec
    with DbInitSpec
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator
  )

  "run" should {

    "do nothing if the 'event' table already exists" in new TestCase {

      createEventTable()

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'project_path' column adding skipped"))
    }

    "do nothing if the 'project_path' column already exists" in new TestCase {

      checkColumnExists shouldBe false

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      checkColumnExists shouldBe true

      logger.loggedOnly(Info("'project_path' column added"))

      logger.reset()

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'project_path' column exists"))
    }

    "add the 'project_path' column if does not exist and migrate the data for it" in new TestCase {

      checkColumnExists shouldBe false

      val event1 = newOrSkippedEvents.generateOne
      storeEvent(event1)
      val event2 = newOrSkippedEvents.generateOne
      storeEvent(event2)

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      findProjectPaths shouldBe Set(event1.project.path, event2.project.path)

      verifyTrue(sql"DROP INDEX idx_project_path;".command)

      eventually {
        logger.loggedOnly(Info("'project_path' column added"))
      }
    }
  }

  private trait TestCase {
    val logger           = TestLogger[IO]()
    val projectPathAdder = new ProjectPathAdderImpl[IO](sessionResource, logger)
  }

  private def checkColumnExists: Boolean = sessionResource
    .useK {
      Kleisli { session =>
        val query: Query[Void, projects.Path] = sql"select project_path from event_log limit 1"
          .query(projectPathDecoder)
        session
          .option(query)
          .map(_ => true)
          .recover { case _ => false }
      }
    }
    .unsafeRunSync()

  private def storeEvent(event: Event): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[EventId ~ projects.Id ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ String] =
        sql"""insert into
            event_log (event_id, project_id, status, created_date, execution_date, event_date, event_body) 
            values (
            $eventIdEncoder, 
            $projectIdEncoder, 
            $eventStatusEncoder, 
            $createdDateEncoder, 
            $executionDateEncoder, 
            $eventDateEncoder, 
            $text)
        """.command

      session
        .prepare(query)
        .use(
          _.execute(
            event.id ~ event.project.id ~ eventStatuses.generateOne ~ createdDates.generateOne ~ executionDates.generateOne ~ eventDates.generateOne ~ toJson(
              event
            )
          )
        )
        .map(_ => ())
    }
  }

  private def toJson(event: Event): String =
    json"""{
    "project": {
      "id": ${event.project.id.value},
      "path": ${event.project.path.value}
     }
  }""".noSpaces

  private def findProjectPaths: Set[Path] = sessionResource
    .useK {
      Kleisli { session =>
        val query: Query[Void, projects.Path] = sql"select project_path from event_log".query(projectPathDecoder)
        session.execute(query)
      }
    }
    .unsafeRunSync()
    .toSet

}
