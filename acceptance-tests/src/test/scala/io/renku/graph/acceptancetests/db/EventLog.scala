/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.graph.acceptancetests.db

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.IO._
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.renku.db.{DBConfigProvider, SessionResource}
import io.renku.eventlog._
import io.renku.events.CategoryName
import io.renku.graph.model.events.{CommitId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.text.varchar
import skunk.implicits._

import scala.util.Try

object EventLog extends TypeSerializers {

  def findEvents(projectId: GitLabId)(implicit ioRuntime: IORuntime): List[(EventId, EventStatus)] = execute {
    session =>
      val query: Query[projects.GitLabId, (EventId, EventStatus)] =
        sql"""SELECT event_id, status
            FROM event
            WHERE project_id = $projectIdEncoder"""
          .query(eventIdDecoder ~ eventStatusDecoder)
          .map { case id ~ status => (id, status) }
      session.prepare(query).flatMap(_.stream(projectId, 32).compile.toList)
  }

  def findEvents(projectId: GitLabId, status: EventStatus*)(implicit ioRuntime: IORuntime): List[CommitId] = execute {
    session =>
      val query: Query[projects.GitLabId, CommitId] = sql"""
            SELECT event_id
            FROM event
            WHERE project_id = $projectIdEncoder AND #${`status IN`(status.toList)}"""
        .query(eventIdDecoder)
        .map(eventId => CommitId(eventId.value))
      session.prepare(query).flatMap(_.stream(projectId, 32).compile.toList)
  }

  def findSyncEventsIO(projectId: GitLabId): IO[List[CategoryName]] =
    sessionResource.flatMap(_.session).use { session =>
      val query: Query[projects.GitLabId, CategoryName] = sql"""
          SELECT category_name
          FROM subscription_category_sync_time
          WHERE project_id = $projectIdEncoder"""
        .query(varchar)
        .map(category => CategoryName(category))
      session.prepare(query).flatMap(_.stream(projectId, 32).compile.toList)
    }

  def findSyncEvents(projectId: GitLabId)(implicit ioRuntime: IORuntime): List[CategoryName] =
    findSyncEventsIO(projectId).unsafeRunSync()

  def findTSMigrationsStatus: IO[Option[MigrationStatus]] = {

    val query: Query[Void, MigrationStatus] =
      sql"""SELECT status
            FROM ts_migration
            ORDER BY change_date DESC
            LIMIT 1"""
        .query(varchar)
        .map(MigrationStatus.apply _)

    sessionResource.flatMap(_.session).use { session =>
      session.prepare(query).flatMap(_.option(Void))
    }
  }

  def forceCategoryEventTriggering(categoryName: CategoryName, projectId: projects.GitLabId)(implicit
      ioRuntime: IORuntime
  ): Unit = execute { session =>
    val query: Command[projects.GitLabId *: String *: EmptyTuple] = sql"""
      DELETE FROM subscription_category_sync_time 
      WHERE project_id = $projectIdEncoder AND category_name = $varchar
      """.command
    session.prepare(query).flatMap(_.execute(projectId *: categoryName.show *: EmptyTuple)).void
  }

  private def `status IN`(status: List[EventStatus]) =
    s"status IN (${NonEmptyList.fromListUnsafe(status).map(el => s"'$el'").toList.mkString(",")})"

  def execute[O](query: Session[IO] => IO[O])(implicit ioRuntime: IORuntime): O =
    sessionResource
      .use(_.useK(Kleisli[IO, Session[IO], O](session => query(session))))
      .unsafeRunSync()

  private lazy val dbConfig: DBConfigProvider.DBConfig[EventLogDB] =
    new EventLogDbConfigProvider[Try].get().fold(throw _, identity)

  def removeGlobalCommitSyncRow(projectId: GitLabId)(implicit ioRuntime: IORuntime): Unit = execute { session =>
    val command: Command[projects.GitLabId] = sql"""
      DELETE FROM subscription_category_sync_time
      WHERE project_id = $projectIdEncoder 
        AND category_name = 'GLOBAL_COMMIT_SYNC' """.command
    session.prepare(command).flatMap(_.execute(projectId)).void
  }

  def startDB()(implicit logger: Logger[IO]): IO[Unit] = for {
    _ <- PostgresDB.startPostgres
    _ <- PostgresDB.initializeDatabase(dbConfig)
    _ <- logger.info("event_log DB started")
  } yield ()

  private lazy val sessionResource: Resource[IO, SessionResource[IO, EventLogDB]] =
    PostgresDB.sessionPoolResource(dbConfig)
}
