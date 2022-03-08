/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.init

import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.association.ProjectPathFinder
import io.renku.tokenrepository.repository.deletion.{TokenRemover, TokenRemoverImpl}
import io.renku.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB, TokenRepositoryTypeSerializers}
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

import scala.util.control.NonFatal

private trait ProjectPathAdder[F[_]] {
  def run(): F[Unit]
}

private class ProjectPathAdderImpl[F[_]: Spawn: Logger](
    sessionResource:   SessionResource[F, ProjectsTokensDB],
    accessTokenCrypto: AccessTokenCrypto[F],
    pathFinder:        ProjectPathFinder[F],
    tokenRemover:      TokenRemover[F]
) extends ProjectPathAdder[F]
    with TokenRepositoryTypeSerializers {

  import accessTokenCrypto._
  import pathFinder._

  def run(): F[Unit] = sessionResource.useK {
    checkColumnExists >>= {
      case true  => Kleisli.liftF(Logger[F].info("'project_path' column exists"))
      case false => addColumn()
    }
  }

  private lazy val checkColumnExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[skunk.Void, projects.Path] = sql"select project_path from projects_tokens limit 1"
      .query(projectPathDecoder)
    Kleisli(_.option(query).map(_ => true).recover { case _ => false })
  }

  private def addColumn(): Kleisli[F, Session[F], Unit] = Kleisli { implicit session =>
    {
      for {
        _ <- execute(sql"ALTER TABLE projects_tokens ADD COLUMN IF NOT EXISTS project_path VARCHAR".command)
        _ <- Spawn[F] start addMissingPaths()
      } yield ()
    } recoverWith logging
  }

  private def addMissingPaths(): F[Unit] = sessionResource.useK {
    Kleisli { implicit session =>
      for {
        _ <- addPathIfMissing().run(session)
        _ <- execute(sql"ALTER TABLE projects_tokens ALTER COLUMN project_path SET NOT NULL".command)
        _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON projects_tokens(project_path)".command)
        _ <- Logger[F].info("'project_path' column added")
      } yield ()
    }
  }

  private def addPathIfMissing(): Kleisli[F, Session[F], Unit] =
    findEntryWithoutPath >>= {
      case None                              => Kleisli.pure(())
      case Some((projectId, encryptedToken)) => addPathOrRemoveRow(projectId, encryptedToken)
    }

  private def findEntryWithoutPath: Kleisli[F, Session[F], Option[(Id, EncryptedAccessToken)]] = {
    val query: Query[Void, (Id, EncryptedAccessToken)] =
      sql"SELECT project_id, token FROM projects_tokens WHERE project_path IS NULL LIMIT 1;"
        .query(projectIdDecoder ~ encryptedAccessTokenDecoder)
        .map { case id ~ token => (id, token) }
    Kleisli(_.option(query))
  }

  private def addPathOrRemoveRow(id: Id, encryptedToken: EncryptedAccessToken) = {
    for {
      token            <- Kleisli.liftF(decrypt(encryptedToken))
      maybeProjectPath <- Kleisli.liftF(findProjectPath(id, Some(token)))
      _                <- addOrRemove(id, maybeProjectPath)
      _                <- addPathIfMissing()
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    Logger[F].error(exception)(s"Error while adding Project Path for projectId = $id")
    addPathIfMissing()
  }

  private def addOrRemove(id: Id, maybePath: Option[Path]): Kleisli[F, Session[F], Unit] =
    maybePath match {
      case Some(path) => addPath(id, path)
      case None       => Kleisli.liftF(tokenRemover.delete(id))
    }

  private def addPath(id: Id, path: Path): Kleisli[F, Session[F], Unit] = {
    val query: Command[Path ~ Id] =
      sql"update projects_tokens set project_path = $projectPathEncoder where project_id = $projectIdEncoder".command
    Kleisli(_.prepare(query).use(_.execute(path ~ id)).void)
  }

  private def execute(sql: Command[Void])(implicit session: Session[F]): F[Unit] =
    session.execute(sql).void

  private lazy val logging: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)("'project_path' column adding failure")
    exception.raiseError[F, Unit]
  }
}

private object ProjectPathAdder {

  def apply[F[_]: Async: Logger](
      sessionResource:  SessionResource[F, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[F]
  ): F[ProjectPathAdder[F]] = for {
    accessTokenCrypto <- AccessTokenCrypto[F]()
    pathFinder        <- ProjectPathFinder[F]
    tokenRemover = new TokenRemoverImpl[F](sessionResource, queriesExecTimes)
  } yield new ProjectPathAdderImpl[F](sessionResource, accessTokenCrypto, pathFinder, tokenRemover)
}
