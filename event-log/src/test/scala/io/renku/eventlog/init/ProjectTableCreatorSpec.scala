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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.{EventDate, InMemoryEventLogDbSpec}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectTableCreatorSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

  import Tables._

  "run" should {

    "create the 'project' table, " +
      "fill it in with data fetched from the 'event_log' about project and the most recent event_date of all project's events" in new TestCase {

        val project1Id   = projectIds.generateOne
        val project1Path = projectPaths.generateOne

        val (_, _, project1EventDate1)                    = createEvent(project1Id, project1Path)
        val (_, _, project1EventDate2)                    = createEvent(project1Id, project1Path)
        val (project2Id, project2Path, project2EventDate) = createEvent()

        tableExists(project) shouldBe false

        tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

        tableExists(project) shouldBe true

        logger.loggedOnly(Info("'project' table created"), Info("'project' table filled in"))

        fetchProjectData should contain theSameElementsAs List(
          (project1Id, project1Path, Set(project1EventDate1, project1EventDate2).maxBy(_.value)),
          (project2Id, project2Path, project2EventDate)
        )
      }

    "create indices for all the columns" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists(project) shouldBe true

      verifyTrue(sql"DROP INDEX idx_project_id;")
      verifyTrue(sql"DROP INDEX idx_project_path;")
      verifyTrue(sql"DROP INDEX idx_latest_event_date;")
    }

    "do nothing if the 'project' table already exists" in new TestCase {
      tableExists(project) shouldBe false

      createEvent()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      fetchProjectData should have size 1

      logger.loggedOnly(Info("'project' table created"), Info("'project' table filled in"))
      logger.reset()

      createEvent()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      fetchProjectData should have size 1

      logger.loggedOnly(Info("'project' table exists"))
    }
  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val tableCreator = new ProjectTableCreatorImpl[IO](transactor, logger)
  }

  private def fetchProjectData: List[(Id, Path, EventDate)] = execute {
    sql"""select project_id, project_path, latest_event_date from project"""
      .query[(Id, Path, EventDate)]
      .to[List]
  }

  private def createEvent(projectId:   Id = projectIds.generateOne,
                          projectPath: Path = projectPaths.generateOne,
                          eventDate:   EventDate = eventDates.generateOne
  ): (Id, Path, EventDate) = {
    insertEvent(
      compoundEventIds.generateOne.copy(projectId = projectId),
      eventStatuses.generateOne,
      executionDates.generateOne,
      eventDate,
      eventBodies.generateOne,
      createdDates.generateOne,
      batchDates.generateOne,
      projectPath,
      maybeMessage = None
    )

    (projectId, projectPath, eventDate)
  }

  protected override def prepareDbForTest(): Unit = {
    super.prepareDbForTest()
    dropTable(project)
  }
}
