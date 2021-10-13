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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration
package triplesgeneration.renkulog

import cats.Applicative
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.config.GitLabUrlLoader
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog.Commands.{GitLabRepoUrlFinder, RepositoryPath}
import io.renku.jsonld.JsonLD

import java.security.SecureRandom
import scala.concurrent.ExecutionContext

private[awaitinggeneration] class RenkuLogTriplesGenerator private[renkulog] (
    gitRepoUrlFinder:    GitLabRepoUrlFinder[IO],
    renku:               Commands.Renku,
    file:                Commands.File,
    git:                 Commands.Git,
    randomLong:          () => Long
)(implicit contextShift: ContextShift[IO])
    extends TriplesGenerator[IO] {

  private val applicative = Applicative[IO]

  import ammonite.ops.{Path, root}
  import applicative._
  import file._
  import gitRepoUrlFinder._

  private val workDirectory: Path = root / "tmp"
  private val repositoryDirectoryFinder = ".*/(.*)$".r
  private val gitAttributeFileName      = ".gitattributes"

  override def generateTriples(
      commitEvent:             CommitEvent
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[IO, ProcessingRecoverableError, JsonLD] =
    EitherT {
      createRepositoryDirectory(commitEvent.project.path)
        .bracket(path => cloneCheckoutGenerate(commitEvent)(maybeAccessToken, RepositoryPath(path)))(deleteDirectory)
        .recoverWith(meaningfulError(commitEvent, maybeAccessToken))
    }

  private def cloneCheckoutGenerate(commitEvent: CommitEvent)(implicit
      maybeAccessToken:                          Option[AccessToken],
      repoDirectory:                             RepositoryPath
  ): IO[Either[ProcessingRecoverableError, JsonLD]] = {
    for {
      _      <- prepareRepository(commitEvent)
      _      <- EitherT.liftF(cleanUpRepository())
      result <- migrateAndLog(commitEvent)
    } yield result
  }.value

  private def prepareRepository(commitEvent: CommitEvent)(implicit
      maybeAccessToken:                      Option[AccessToken],
      repoDirectory:                         RepositoryPath
  ): EitherT[IO, ProcessingRecoverableError, Unit] = for {
    repositoryUrl <- EitherT.liftF(findRepositoryUrl(commitEvent.project.path, maybeAccessToken))
    _             <- git.clone(repositoryUrl, workDirectory)
    _             <- EitherT.liftF(git.checkout(commitEvent.commitId))
  } yield ()

  private def cleanUpRepository()(implicit repoDirectory: RepositoryPath) = {
    val gitAttributeFilePath = repoDirectory.value / gitAttributeFileName
    file.exists(gitAttributeFilePath).flatMap {
      case false => IO.unit
      case true =>
        for {
          repoDirty <- git.status.map(status => !status.contains("nothing to commit"))
          _         <- whenA(repoDirty)(git.rm(gitAttributeFilePath))
          _         <- whenA(repoDirty)(git.`reset --hard`)
        } yield ()
    }
  }

  private def migrateAndLog(
      commitEvent:          CommitEvent
  )(implicit repoDirectory: RepositoryPath): EitherT[IO, ProcessingRecoverableError, JsonLD] = for {
    _       <- EitherT.liftF(renku migrate commitEvent)
    triples <- renku.`export`
  } yield triples

  private def createRepositoryDirectory(projectPath: projects.Path): IO[Path] =
    mkdir(tempDirectoryName(repositoryNameFrom(projectPath)))

  private def tempDirectoryName(repositoryName: String) =
    workDirectory / s"$repositoryName-${randomLong()}"

  private def repositoryNameFrom(projectPath: projects.Path): String = projectPath.value match {
    case repositoryDirectoryFinder(folderName) => folderName
  }

  private def meaningfulError(
      commitEvent:      CommitEvent,
      maybeAccessToken: Option[AccessToken]
  ): PartialFunction[Throwable, IO[Either[ProcessingRecoverableError, JsonLD]]] = { case exception =>
    IO.raiseError {
      (Option(exception.getMessage) -> maybeAccessToken)
        .mapN { (message, token) =>
          if (message contains token.value)
            new Exception(
              s"${logMessageCommon(commitEvent)} triples generation failed: ${message.replaceAll(token.value, token.toString)}"
            )
          else
            new Exception(s"${logMessageCommon(commitEvent)} triples generation failed", exception)
        }
        .getOrElse(new Exception(s"${logMessageCommon(commitEvent)} triples generation failed", exception))
    }
  }
}

private[events] object RenkuLogTriplesGenerator {

  def apply()(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[TriplesGenerator[IO]] = for {
    gitLabUrl <- GitLabUrlLoader[IO]()
  } yield new RenkuLogTriplesGenerator(
    new GitLabRepoUrlFinder[IO](gitLabUrl),
    new Commands.Renku,
    new Commands.File,
    new Commands.Git,
    randomLong = new SecureRandom().nextLong _
  )
}
