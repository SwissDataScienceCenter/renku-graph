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
import io.renku.eventlog.EventDate
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectTableCreatorSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    latestEventDatesViewRemover
  )

  "run" should {

    "do nothing if the 'event' table already exists" in new TestCase {

      createEventTable()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'project' table creation skipped"))
    }

    "create the 'project' table, " +
      "fill it in with data fetched from the 'event_log' about project and the most recent event_date of all project's events" in new TestCase {

        val project1Id    = projectIds.generateOne
        val project1Path  = projectPaths.generateOne
        val project2Id    = projectIds.generateOne
        val project2Path1 = projectPaths.generateOne
        val project2Path2 = projectPaths.generateOne

        val (_, _, project1EventDate1)                    = createEvent(project1Id, project1Path)
        val (_, _, project1EventDate2)                    = createEvent(project1Id, project1Path)
        val (_, _, project2EventDate1)                    = createEvent(project2Id, project2Path1)
        val (_, _, project2EventDate2)                    = createEvent(project2Id, project2Path2)
        val (project3Id, project3Path, project3EventDate) = createEvent()

        tableExists("project") shouldBe false

        tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

        tableExists("project") shouldBe true

        logger.loggedOnly(Info("'project' table created"), Info("'project' table filled in"))

        val project2Path =
          if ((project2EventDate1.value compareTo project2EventDate2.value) < 0) project2Path2
          else project2Path1
        fetchProjectData should contain theSameElementsAs List(
          (project1Id, project1Path, Set(project1EventDate1, project1EventDate2).maxBy(_.value)),
          (project2Id, project2Path, Set(project2EventDate1, project2EventDate2).maxBy(_.value)),
          (project3Id, project3Path, project3EventDate)
        )
      }

    "create indices for all the columns" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("project") shouldBe true

      verifyTrue(sql"DROP INDEX idx_project_id;")
      verifyTrue(sql"DROP INDEX idx_project_path;")
      verifyTrue(sql"DROP INDEX idx_latest_event_date;")
    }

    "do nothing if the 'project' table already exists" in new TestCase {

      tableExists("project") shouldBe false

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
    execute {
      sql"""|insert into
            |event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, batch_date, event_body)
            |values (${eventIds.generateOne}, $projectId, $projectPath, ${eventStatuses.generateOne}, ${createdDates.generateOne}, ${executionDates.generateOne}, $eventDate, ${batchDates.generateOne}, ${eventBodies.generateOne})
      """.stripMargin.update.run.map(_ => ())
    }

    (projectId, projectPath, eventDate)
  }
}
