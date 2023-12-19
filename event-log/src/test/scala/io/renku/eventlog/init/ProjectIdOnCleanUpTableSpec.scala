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
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.implicits._

class ProjectIdOnCleanUpTableSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with DbInitSpec
    with should.Matchers
    with TypeSerializers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[ProjectIdOnCleanUpTable[IO]]

  it should "add 'project_id' column to the 'clean_up_events_queue' if doesn't exist" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- verifyColumnExists("clean_up_events_queue", "project_id").asserting(_ shouldBe false)

        _ <- migrator.run.assertNoException

        _ <- verifyColumnExists("clean_up_events_queue", "project_id").asserting(_ shouldBe true)

        _ <- logger.loggedOnlyF(Info("'clean_up_events_queue.project_id' column added"))

        _ <- logger.resetF()

        _ <- migrator.run.assertNoException

        _ <- logger.loggedOnlyF(Info("'clean_up_events_queue.project_id' column exists"))
      } yield Succeeded
  }

  it should "fill in the new 'project_id' column with data from the 'project' table " +
    "and remove rows without matching project" in testDBResource.use { implicit cfg =>
      val project1 = consumerProjects.generateOne
      for {
        _ <- insertToQueue(project1.slug)
        _ <- insertToProject(project1)

        project2 = consumerProjects.generateOne
        _ <- insertToQueue(project2.slug)

        _ <- migrator.run.assertNoException

        _ <- findQueueRows.asserting(_ shouldBe List(project1))
      } yield Succeeded
    }

  private def migrator(implicit cfg: DBConfig[EventLogDB]) = new ProjectIdOnCleanUpTableImpl[IO]

  private def insertToQueue(slug: projects.Slug)(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    executeCommand {
      sql"""INSERT INTO clean_up_events_queue(date, project_path)
          VALUES(now(), '#${slug.show}')
       """.command
    }

  private def insertToProject(project: Project)(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    executeCommand {
      sql"""INSERT INTO project(project_id, project_path, latest_event_date)
          VALUES (#${project.id.value.toString}, '#${project.slug.value}', now())
       """.command
    }

  private def findQueueRows(implicit cfg: DBConfig[EventLogDB]): IO[List[Project]] =
    moduleSessionResource.session.use { session =>
      val query: Query[Void, Project] = sql"""
          SELECT project_path, project_id 
          FROM clean_up_events_queue"""
        .query(projectSlugDecoder ~ projectIdDecoder)
        .map { case (slug: projects.Slug, id: projects.GitLabId) => Project(id, slug) }
      session.execute(query)
    }
}
