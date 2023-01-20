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
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.circe.parser._
import io.circe.{Decoder, HCursor}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{GitLabId, Path}
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.util.control.NonFatal

private trait ProjectPathAdder[F[_]] extends DbMigrator[F]

private object ProjectPathAdder {
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: ProjectPathAdder[F] = new ProjectPathAdderImpl[F]
}

private class ProjectPathAdderImpl[F[_]: MonadCancelThrow: Logger: SessionResource]
    extends ProjectPathAdder[F]
    with TypeSerializers {

  import MigratorTools._

  override def run(): F[Unit] = SessionResource[F].useK {
    whenTableExists("event")(
      Kleisli.liftF(Logger[F] info "'project_path' column adding skipped"),
      otherwise = checkColumnExists("event_log", "project_path") >>= {
        case true  => Kleisli.liftF(Logger[F] info "'project_path' column exists")
        case false => addColumn()
      }
    )
  }

  private def addColumn(): Kleisli[F, Session[F], Unit] = {
    for {
      _                  <- execute(sql"ALTER TABLE event_log ADD COLUMN IF NOT EXISTS project_path VARCHAR".command)
      projectIdsAndPaths <- findDistinctProjects
      _                  <- updatePaths(projectIdsAndPaths)
      _                  <- execute(sql"ALTER TABLE event_log ALTER COLUMN project_path SET NOT NULL".command)
      _                  <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON event_log(project_path)".command)
      _                  <- Kleisli.liftF(Logger[F].info("'project_path' column added"))
    } yield ()
  } recoverWith logging

  private lazy val findDistinctProjects: Kleisli[F, Session[F], List[(GitLabId, Path)]] = {
    val query: Query[Void, String] = sql"select min(event_body) from event_log group by project_id;".query(text)
    Kleisli(_.execute(query).flatMap(toListOfProjectIdAndPath))
  }

  private def toListOfProjectIdAndPath(bodies: List[String]): F[List[(GitLabId, Path)]] =
    bodies.map(parseToProjectIdAndPath).sequence

  private def parseToProjectIdAndPath(body: String): F[(GitLabId, Path)] =
    MonadCancelThrow[F].fromEither {
      for {
        json  <- parse(body)
        tuple <- json.as[(GitLabId, Path)]
      } yield tuple
    }

  private implicit lazy val projectIdAndPathDecoder: Decoder[(GitLabId, Path)] = (cursor: HCursor) =>
    for {
      id   <- cursor.downField("project").downField("id").as[GitLabId]
      path <- cursor.downField("project").downField("path").as[Path]
    } yield id -> path

  private def updatePaths(
      projectIdsAndPaths: List[(GitLabId, Path)]
  ): Kleisli[F, Session[F], Unit] =
    projectIdsAndPaths
      .map(toSqlUpdate)
      .sequence
      .map(_ => ())

  private lazy val toSqlUpdate: ((GitLabId, Path)) => Kleisli[F, Session[F], Unit] = { case (projectId, projectPath) =>
    val query: Command[projects.Path ~ projects.GitLabId] =
      sql"update event_log set project_path = $projectPathEncoder where project_id = $projectIdEncoder".command
    Kleisli(_.prepare(query).flatMap(_.execute(projectPath ~ projectId)).void)
  }

  private lazy val logging: PartialFunction[Throwable, Kleisli[F, Session[F], Unit]] = { case NonFatal(exception) =>
    Kleisli.liftF(
      Logger[F]
        .error(exception)("'project_path' column adding failure")
        .flatMap(_ => exception.raiseError[F, Unit])
    )
  }
}
