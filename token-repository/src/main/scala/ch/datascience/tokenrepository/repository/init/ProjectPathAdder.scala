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
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.association.{IOProjectPathFinder, ProjectPathFinder}
import ch.datascience.tokenrepository.repository.deletion.TokenRemover
import ch.datascience.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB}
import doobie.implicits._
import doobie.util.fragment.Fragment
import io.chrisdavenport.log4cats.Logger

import scala.util.control.NonFatal

private trait ProjectPathAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class IOProjectPathAdder(
    transactor:        SessionResource[IO, ProjectsTokensDB],
    accessTokenCrypto: AccessTokenCrypto[IO],
    pathFinder:        ProjectPathFinder[IO],
    tokenRemover:      TokenRemover[IO],
    logger:            Logger[IO]
)(implicit ME:         Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends ProjectPathAdder[IO] {

  import accessTokenCrypto._
  import pathFinder._

  def run(): IO[Unit] =
    checkColumnExists flatMap {
      case true  => logger.info("'project_path' column exists")
      case false => addColumn()
    }

  private def checkColumnExists: IO[Boolean] =
    sql"select project_path from projects_tokens limit 1"
      .query[String]
      .option
      .transact(transactor.resource)
      .map(_ => true)
      .recover { case _ => false }

  private def addColumn() = {
    for {
      _ <- execute(sql"ALTER TABLE projects_tokens ADD COLUMN IF NOT EXISTS project_path VARCHAR", transactor)
      _ <- addMissingPaths().start
    } yield ()
  } recoverWith logging

  private def addMissingPaths(): IO[Unit] =
    for {
      _ <- addPathIfMissing()
      _ <- execute(sql"ALTER TABLE projects_tokens ALTER COLUMN project_path SET NOT NULL", transactor)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON projects_tokens(project_path)", transactor)
      _ <- logger.info("'project_path' column added")
    } yield ()

  private def addPathIfMissing(): IO[Unit] =
    findEntryWithoutPath flatMap {
      case None                              => ME.unit
      case Some((projectId, encryptedToken)) => addPathOrRemoveRow(projectId, encryptedToken)
    }

  private def findEntryWithoutPath =
    sql"select project_id, token from projects_tokens where project_path IS NULL limit 1;"
      .query[(Int, String)]
      .option
      .transact(transactor.resource)
      .flatMap {
        case None =>
          Option.empty[(Id, EncryptedAccessToken)].pure[IO]
        case Some((id, token)) =>
          ME.fromEither((Id from id, EncryptedAccessToken from token).mapN { case (projectId, encryptedToken) =>
            Option(projectId -> encryptedToken)
          })
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

  private def addOrRemove(id: Id, maybePath: Option[Path]): IO[Unit] =
    maybePath match {
      case Some(path) => addPath(id, path)
      case None       => tokenRemover.delete(id)
    }

  private def addPath(id: Id, path: Path): IO[Unit] =
    sql"update projects_tokens set project_path = ${path.value} where project_id = ${id.value}".update.run
      .transact(transactor.resource)
      .map(_ => ())

  private def execute(sql: Fragment, transactor: SessionResource[IO, ProjectsTokensDB]): IO[Unit] =
    sql.update.run
      .transact(transactor.resource)
      .map(_ => ())

  private lazy val logging: PartialFunction[Throwable, IO[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("'project_path' column adding failure")
    ME.raiseError(exception)
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
    } yield new IOProjectPathAdder(transactor, accessTokenCrypto, pathFinder, tokenRemover, logger)
}
