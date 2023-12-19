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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.EventLogDB
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events._
import io.renku.graph.model.projects.{GitLabId, Slug}
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.codec.all._
import skunk.implicits._

import java.time.ZoneOffset

class ProjectTableCreatorSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[ProjectTableCreator[IO]]

  it should "do nothing if the 'event' table already exists" in testDBResource.use { implicit cfg =>
    for {
      _ <- createEventTable >> logger.resetF()

      _ <- tableCreator.run.assertNoException

      _ <- logger.loggedOnlyF(Info("'project' table creation skipped"))
    } yield Succeeded
  }

  it should "create the 'project' table, " +
    "fill it in with data fetched from the 'event_log' about project and the most recent event_date of all project's events" in testDBResource
      .use { implicit cfg =>
        val project1Id    = projectIds.generateOne
        val project1Slug  = projectSlugs.generateOne
        val project2Id    = projectIds.generateOne
        val project2Slug1 = projectSlugs.generateOne
        val project2Slug2 = projectSlugs.generateOne

        for {
          (_, _, project1EventDate1)                    <- createEvent(project1Id, project1Slug)
          (_, _, project1EventDate2)                    <- createEvent(project1Id, project1Slug)
          (_, _, project2EventDate1)                    <- createEvent(project2Id, project2Slug1)
          (_, _, project2EventDate2)                    <- createEvent(project2Id, project2Slug2)
          (project3Id, project3Slug, project3EventDate) <- createEvent()

          _ <- tableExists("project").asserting(_ shouldBe false)

          _ <- tableCreator.run.assertNoException

          _ <- tableExists("project").asserting(_ shouldBe true)

          _ <- logger.loggedOnlyF(Info("'project' table created"), Info("'project' table filled in"))

          project2Slug =
            if ((project2EventDate1 compareTo project2EventDate2) < 0) project2Slug2
            else project2Slug1
          _ <- fetchProjectData.asserting(
                 _ should contain theSameElementsAs List(
                   (project1Id, project1Slug, Set(project1EventDate1, project1EventDate2).max),
                   (project2Id, project2Slug, Set(project2EventDate1, project2EventDate2).max),
                   (project3Id, project3Slug, project3EventDate)
                 )
               )
        } yield Succeeded
      }

  it should "create indices for all the columns" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableCreator.run.assertNoException

      _ <- tableExists("project").asserting(_ shouldBe true)

      _ <- verifyIndexExists("project", "idx_project_project_id").asserting(_ shouldBe true)
      _ <- verifyIndexExists("project", "idx_project_project_path").asserting(_ shouldBe true)
      _ <- verifyIndexExists("project", "idx_project_latest_event_date").asserting(_ shouldBe true)
    } yield Succeeded
  }

  it should "do nothing if the 'project' table already exists" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableExists("project").asserting(_ shouldBe false)

      _ <- createEvent()

      _ <- tableCreator.run.assertNoException

      _ <- fetchProjectData.asserting(_ should have size 1)

      _ <- logger.loggedOnlyF(Info("'project' table created"), Info("'project' table filled in"))

      _ <- logger.resetF()

      _ <- tableCreator.run.assertNoException

      _ <- fetchProjectData.asserting(_ should have size 1)

      _ <- logger.loggedOnlyF(Info("'project' table exists"))
    } yield Succeeded
  }

  private def tableCreator(implicit cfg: DBConfig[EventLogDB]) = new ProjectTableCreatorImpl[IO]

  private def fetchProjectData(implicit cfg: DBConfig[EventLogDB]): IO[List[(GitLabId, Slug, EventDate)]] =
    moduleSessionResource.session.use { session =>
      val query: Query[Void, (GitLabId, Slug, EventDate)] =
        sql"""select project_id, project_path, latest_event_date from project"""
          .query(projectIdDecoder ~ projectSlugDecoder ~ eventDateTimestampDecoder)
          .map { case projectId ~ projectSlug ~ eventDate =>
            (projectId, projectSlug, eventDate)
          }
      session.execute(query)
    }

  private val eventDateTimestampDecoder: Decoder[EventDate] =
    timestamp.map(timestamp => EventDate(timestamp.toInstant(ZoneOffset.UTC)))

  private def createEvent(projectId:   GitLabId = projectIds.generateOne,
                          projectSlug: Slug = projectSlugs.generateOne,
                          eventDate:   EventDate = eventDates.generateOne
  )(implicit cfg: DBConfig[EventLogDB]): IO[(GitLabId, Slug, EventDate)] =
    moduleSessionResource.session
      .use { session =>
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
      .as((projectId, projectSlug, eventDate))
}
