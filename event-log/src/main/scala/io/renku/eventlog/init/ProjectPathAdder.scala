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
import io.circe.Decoder
import io.circe.parser._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{GitLabId, Slug}
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

  override def run: F[Unit] = SessionResource[F].useK {
    whenTableExists("event")(
      Kleisli.liftF(Logger[F] info "'project_path' column adding skipped"),
      otherwise = checkColumnExists("projects_tokens", "project_slug") >>= {
        case true => Kleisli.liftF(Logger[F].info("no need to create 'project_path' as 'project_slug' already exists"))
        case false =>
          checkColumnExists("event_log", "project_path") >>= {
            case true  => Kleisli.liftF(Logger[F] info "'project_path' column exists")
            case false => addColumn()
          }
      }
    )
  }

  private def addColumn(): Kleisli[F, Session[F], Unit] = {
    for {
      _                  <- execute(sql"ALTER TABLE event_log ADD COLUMN IF NOT EXISTS project_path VARCHAR".command)
      projectIdsAndSlugs <- findDistinctProjects
      _                  <- updateSlugs(projectIdsAndSlugs)
      _                  <- execute(sql"ALTER TABLE event_log ALTER COLUMN project_path SET NOT NULL".command)
      _                  <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON event_log(project_path)".command)
      _                  <- Kleisli.liftF(Logger[F].info("'project_path' column added"))
    } yield ()
  } recoverWith logging

  private lazy val findDistinctProjects: Kleisli[F, Session[F], List[(GitLabId, Slug)]] = {
    val query: Query[Void, String] = sql"select min(event_body) from event_log group by project_id;".query(text)
    Kleisli(_.execute(query).flatMap(toListOfProjectIdAndSlug))
  }

  private def toListOfProjectIdAndSlug(bodies: List[String]): F[List[(GitLabId, Slug)]] =
    bodies.map(parseToProjectIdAndSlug).sequence

  private def parseToProjectIdAndSlug(body: String): F[(GitLabId, Slug)] =
    MonadCancelThrow[F].fromEither {
      parse(body) flatMap (_.as[(GitLabId, Slug)])
    }

  private implicit lazy val projectIdAndSlugDecoder: Decoder[(GitLabId, Slug)] = cursor =>
    for {
      id   <- cursor.downField("project").downField("id").as[GitLabId]
      slug <- cursor.downField("project").downField("slug").as[Slug]
    } yield id -> slug

  private def updateSlugs(projectIdsAndSlugs: List[(GitLabId, Slug)]): Kleisli[F, Session[F], Unit] =
    projectIdsAndSlugs
      .map(toSqlUpdate)
      .sequence
      .map(_ => ())

  private lazy val toSqlUpdate: ((GitLabId, Slug)) => Kleisli[F, Session[F], Unit] = { case (projectId, projectSlug) =>
    val query: Command[projects.Slug *: projects.GitLabId *: EmptyTuple] =
      sql"""UPDATE event_log
            SET project_path = $projectSlugEncoder
            WHERE project_id = $projectIdEncoder""".command
    Kleisli(_.prepare(query).flatMap(_.execute(projectSlug *: projectId *: EmptyTuple)).void)
  }

  private lazy val logging: PartialFunction[Throwable, Kleisli[F, Session[F], Unit]] = { case NonFatal(exception) =>
    Kleisli.liftF(
      Logger[F]
        .error(exception)("'project_path' column adding failure")
        .flatMap(_ => exception.raiseError[F, Unit])
    )
  }
}
