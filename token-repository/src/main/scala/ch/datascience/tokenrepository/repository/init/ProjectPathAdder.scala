/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository.init

import cats.effect._
import cats.syntax.all._
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.association.{IOProjectPathFinder, ProjectPathFinder}
import ch.datascience.tokenrepository.repository.deletion.TokenRemover
import ch.datascience.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB, TokenRepositoryTypeSerializers}
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

import scala.util.control.NonFatal

private trait ProjectPathAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class ProjectPathAdderImpl[Interpretation[_]: Concurrent: Bracket[*[_], Throwable]: ContextShift](
    transactor:        SessionResource[Interpretation, ProjectsTokensDB],
    accessTokenCrypto: AccessTokenCrypto[Interpretation],
    pathFinder:        ProjectPathFinder[Interpretation],
    tokenRemover:      TokenRemover[Interpretation],
    logger:            Logger[Interpretation]
) extends ProjectPathAdder[Interpretation]
    with TokenRepositoryTypeSerializers {

  import accessTokenCrypto._
  import pathFinder._

  def run(): Interpretation[Unit] =
    checkColumnExists flatMap {
      case true  => logger.info("'project_path' column exists")
      case false => addColumn()
    }

  private def checkColumnExists: Interpretation[Boolean] = {
    val query: Query[skunk.Void, projects.Path] = sql"select project_path from projects_tokens limit 1"
      .query(projectPathGet)
    transactor.use { session =>
      session
        .option(query)
        .map(_ => true)
        .recover { case _ => false }
    }
  }

  private def addColumn() = {
    for {
      _ <- execute(sql"ALTER TABLE projects_tokens ADD COLUMN IF NOT EXISTS project_path VARCHAR".command, transactor)
      _ <- Concurrent[Interpretation].start(addMissingPaths())
    } yield ()
  } recoverWith logging

  private def addMissingPaths(): Interpretation[Unit] =
    for {
      _ <- addPathIfMissing()
      _ <- execute(sql"ALTER TABLE projects_tokens ALTER COLUMN project_path SET NOT NULL".command, transactor)
      _ <-
        execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON projects_tokens(project_path)".command, transactor)
      _ <- logger.info("'project_path' column added")
    } yield ()

  private def addPathIfMissing(): Interpretation[Unit] =
    findEntryWithoutPath flatMap {
      case None                              => ().pure[Interpretation]
      case Some((projectId, encryptedToken)) => addPathOrRemoveRow(projectId, encryptedToken)
    }

  private def findEntryWithoutPath: Interpretation[Option[(Id, EncryptedAccessToken)]] = {
    val query: Query[Void, (Id, EncryptedAccessToken)] =
      sql"select project_id, token from projects_tokens where project_path IS NULL limit 1;"
        .query(projectIdGet ~ encryptedAccessTokenGet)
        .map { case id ~ token => (id, token) }
    transactor
      .use(session => session.option(query))
  }

  private def addPathOrRemoveRow(id: Id, encryptedToken: EncryptedAccessToken) = {
    for {
      token            <- decrypt(encryptedToken)
      maybeProjectPath <- findProjectPath(id, Some(token))
      _                <- addOrRemove(id, maybeProjectPath)
      _                <- addPathIfMissing()
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    logger.error(exception)(s"Error while adding Project Path for projectId = $id")
    addPathIfMissing()
  }

  private def addOrRemove(id: Id, maybePath: Option[Path]): Interpretation[Unit] =
    maybePath match {
      case Some(path) => addPath(id, path)
      case None       => tokenRemover.delete(id)
    }

  private def addPath(id: Id, path: Path): Interpretation[Unit] =
    transactor.use { session =>
      val query: Command[Path ~ Id] =
        sql"update projects_tokens set project_path = $projectPathPut where project_id = $projectIdPut".command
      session.prepare(query).use(_.execute(path ~ id)).void
    }

  private def execute(sql:        Command[Void],
                      transactor: SessionResource[Interpretation, ProjectsTokensDB]
  ): Interpretation[Unit] =
    transactor.use(session => session.execute(sql).void)

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("'project_path' column adding failure")
    exception.raiseError[Interpretation, Unit]
  }
}

private object IOProjectPathAdder {

  import cats.effect.{ContextShift, IO, Timer}

  import scala.concurrent.ExecutionContext

  def apply(
      transactor:       SessionResource[IO, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
      logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ProjectPathAdder[IO]] =
    for {
      accessTokenCrypto <- AccessTokenCrypto[IO]()
      pathFinder        <- IOProjectPathFinder(logger)
      tokenRemover = new TokenRemover[IO](transactor, queriesExecTimes)
    } yield new ProjectPathAdderImpl[IO](transactor, accessTokenCrypto, pathFinder, tokenRemover, logger)
}
