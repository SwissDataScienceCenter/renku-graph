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

class LatestEventDatesViewCreatorSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

  "run" should {

    "create a new 'project_latest_event_date' view and do nothing if if already exists" in new TestCase {
      checkViewExists shouldBe false

      viewCreator.run().unsafeRunSync() shouldBe ((): Unit)

      checkViewExists shouldBe true

      verifyTrue(sql"DROP INDEX project_latest_event_date_project_idx;")

      logger.loggedOnly(Info("'project_latest_event_date' view created"))

      viewCreator.run().unsafeRunSync() shouldBe ((): Unit)
    }

    "create a view which contains info about project and the latest event_date of all its events" in new TestCase {
      viewCreator.run().unsafeRunSync() shouldBe ((): Unit)

      val project1Id   = projectIds.generateOne
      val project1Path = projectPaths.generateOne

      val (_, _, project1EventDate1)                    = createEvent(project1Id, project1Path)
      val (_, _, project1EventDate2)                    = createEvent(project1Id, project1Path)
      val (project2Id, project2Path, project2EventDate) = createEvent()

      refreshView()

      fetchViewData should contain theSameElementsAs List(
        (project1Id, project1Path, Set(project1EventDate1, project1EventDate2).maxBy(_.value)),
        (project2Id, project2Path, project2EventDate)
      )
    }
  }

  private trait TestCase {
    val logger      = TestLogger[IO]()
    val viewCreator = new LatestEventDatesViewCreatorImpl[IO](transactor, logger)
  }

  private def checkViewExists: Boolean =
    sql"select project_path from project_latest_event_date limit 1"
      .query[String]
      .option
      .transact(transactor.get)
      .map(_ => true)
      .recover { case _ => false }
      .unsafeRunSync()

  private def refreshView(): Unit = verifyTrue {
    sql""" REFRESH MATERIALIZED VIEW CONCURRENTLY project_latest_event_date"""
  }

  private def fetchViewData: List[(Id, Path, EventDate)] = execute {
    fr"""select project_id, project_path, latest_event_date
            from project_latest_event_date"""
      .query[(Id, Path, EventDate)]
      .to[List]
  }

  private def createEvent(projectId:   Id = projectIds.generateOne,
                          projectPath: Path = projectPaths.generateOne,
                          eventDate:   EventDate = eventDates.generateOne
  ): (Id, Path, EventDate) = {
    val eventId = compoundEventIds.generateOne.copy(projectId = projectId)

    storeEvent(
      eventId,
      eventStatuses.generateOne,
      executionDates.generateOne,
      eventDate,
      eventBodies.generateOne,
      batchDate = batchDates.generateOne,
      projectPath = projectPath
    )

    (projectId, projectPath, eventDate)
  }
}
