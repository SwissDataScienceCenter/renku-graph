/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.projects.Path
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import io.circe.literal._
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.Event
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectPathAdderSpec
    extends AnyWordSpec
    with DbInitSpec
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "run" should {

    "do nothing if the 'project_path' column already exists" in new TestCase {
      if (!tableExists()) createTable()
      addProjectPath()
      checkColumnExists shouldBe true

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      checkColumnExists shouldBe true

      logger.loggedOnly(Info("'project_path' column exists"))
    }

    "add the 'project_path' column if does not exist and migrate the data for it" in new TestCase {
      if (tableExists()) {
        dropTable()
        createTable()
      }
      checkColumnExists shouldBe false

      val event1 = events.generateOne
      storeEvent(event1)
      val event2 = events.generateOne
      storeEvent(event2)

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      findProjectPaths shouldBe Set(event1.project.path, event2.project.path)

      verifyTrue(sql"DROP INDEX idx_project_path;")

      eventually {
        logger.loggedOnly(Info("'project_path' column added"))
      }
    }
  }

  private trait TestCase {
    val logger           = TestLogger[IO]()
    val projectPathAdder = new ProjectPathAdder[IO](transactor, logger)
  }

  private def addProjectPath(): Unit = execute {
    sql"""|ALTER TABLE event_log 
          |ADD COLUMN project_path VARCHAR;
       """.stripMargin.update.run.map(_ => ())
  }

  private def checkColumnExists: Boolean =
    sql"select project_path from event_log limit 1"
      .query[String]
      .option
      .transact(transactor.get)
      .map(_ => true)
      .recover { case _ => false }
      .unsafeRunSync()

  private def storeEvent(event: Event): Unit = execute {
    sql"""insert into 
         |event_log (event_id, project_id, status, created_date, execution_date, event_date, event_body) 
         |values (
         |${event.id}, 
         |${event.project.id}, 
         |${eventStatuses.generateOne}, 
         |${createdDates.generateOne}, 
         |${executionDates.generateOne}, 
         |${eventDates.generateOne}, 
         |${toJson(event)})
      """.stripMargin.update.run.map(_ => ())
  }

  private def toJson(event: Event): String = json"""{
    "project": {
      "id": ${event.project.id.value},
      "path": ${event.project.path.value}
     }
  }""".noSpaces

  private def findProjectPaths: Set[Path] =
    sql"select project_path from event_log"
      .query[Path]
      .to[List]
      .transact(transactor.get)
      .unsafeRunSync()
      .toSet
}
