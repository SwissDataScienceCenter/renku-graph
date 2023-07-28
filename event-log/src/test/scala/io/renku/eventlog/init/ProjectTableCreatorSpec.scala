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

import cats.data.Kleisli
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events._
import io.renku.graph.model.projects.{GitLabId, Slug}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.codec.all._
import skunk.implicits._

import java.time.ZoneOffset

class ProjectTableCreatorSpec extends AnyWordSpec with IOSpec with DbInitSpec with should.Matchers {

  protected[init] override lazy val migrationsToRun: List[DbMigrator[IO]] = allMigrations.takeWhile {
    case _: ProjectTableCreatorImpl[IO] => false
    case _ => true
  }

  "run" should {

    "do nothing if the 'event' table already exists" in new TestCase {

      createEventTable()

      tableCreator.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'project' table creation skipped"))
    }

    "create the 'project' table, " +
      "fill it in with data fetched from the 'event_log' about project and the most recent event_date of all project's events" in new TestCase {

        val project1Id    = projectIds.generateOne
        val project1Slug  = projectSlugs.generateOne
        val project2Id    = projectIds.generateOne
        val project2Slug1 = projectSlugs.generateOne
        val project2Slug2 = projectSlugs.generateOne

        val (_, _, project1EventDate1)                    = createEvent(project1Id, project1Slug)
        val (_, _, project1EventDate2)                    = createEvent(project1Id, project1Slug)
        val (_, _, project2EventDate1)                    = createEvent(project2Id, project2Slug1)
        val (_, _, project2EventDate2)                    = createEvent(project2Id, project2Slug2)
        val (project3Id, project3Slug, project3EventDate) = createEvent()

        tableExists("project") shouldBe false

        tableCreator.run.unsafeRunSync() shouldBe ((): Unit)

        tableExists("project") shouldBe true

        logger.loggedOnly(Info("'project' table created"), Info("'project' table filled in"))

        val project2Path =
          if ((project2EventDate1 compareTo project2EventDate2) < 0) project2Slug2
          else project2Slug1
        fetchProjectData should contain theSameElementsAs List(
          (project1Id, project1Slug, Set(project1EventDate1, project1EventDate2).max),
          (project2Id, project2Path, Set(project2EventDate1, project2EventDate2).max),
          (project3Id, project3Slug, project3EventDate)
        )
      }

    "create indices for all the columns" in new TestCase {

      tableCreator.run.unsafeRunSync() shouldBe ((): Unit)

      tableExists("project") shouldBe true

      verifyTrue(sql"DROP INDEX idx_project_id;".command)
      verifyTrue(sql"DROP INDEX idx_project_path;".command)
      verifyTrue(sql"DROP INDEX idx_latest_event_date;".command)
    }

    "do nothing if the 'project' table already exists" in new TestCase {

      tableExists("project") shouldBe false

      createEvent()

      tableCreator.run.unsafeRunSync() shouldBe ((): Unit)

      fetchProjectData should have size 1

      logger.loggedOnly(Info("'project' table created"), Info("'project' table filled in"))

      logger.reset()

      tableCreator.run.unsafeRunSync() shouldBe ((): Unit)

      fetchProjectData should have size 1

      logger.loggedOnly(Info("'project' table exists"))
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tableCreator = new ProjectTableCreatorImpl[IO]
  }

  private def fetchProjectData: List[(GitLabId, Slug, EventDate)] = execute {
    Kleisli { session =>
      val query: Query[Void, (GitLabId, Slug, EventDate)] =
        sql"""select project_id, project_path, latest_event_date from project"""
          .query(projectIdDecoder ~ projectSlugDecoder ~ eventDateTimestampDecoder)
          .map { case projectId ~ projectSlug ~ eventDate =>
            (projectId, projectSlug, eventDate)
          }
      session.execute(query)
    }
  }

  private val eventDateTimestampDecoder: Decoder[EventDate] =
    timestamp.map(timestamp => EventDate(timestamp.toInstant(ZoneOffset.UTC)))

  private def createEvent(projectId:   GitLabId = projectIds.generateOne,
                          projectSlug: Slug = projectSlugs.generateOne,
                          eventDate:   EventDate = eventDates.generateOne
  ): (GitLabId, Slug, EventDate) = {
    execute[Unit] {
      Kleisli { session =>
        val query: Command[
          EventId *: GitLabId *: Slug *: EventStatus *: CreatedDate *: ExecutionDate *: EventDate *: BatchDate *: EventBody *: EmptyTuple
        ] = sql"""
            insert into
            event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, batch_date, event_body)
            values ($eventIdEncoder, $projectIdEncoder, $projectSlugEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder)
      """.command
        session
          .prepare(query)
          .flatMap(
            _.execute(
              eventIds.generateOne *: projectId *: projectSlug *: eventStatuses.generateOne *: createdDates.generateOne *: executionDates.generateOne *: eventDate *: batchDates.generateOne *: eventBodies.generateOne *: EmptyTuple
            )
          )
          .map(_ => ())
      }
    }

    (projectId, projectSlug, eventDate)
  }
}
