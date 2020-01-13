/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog

import java.security.SecureRandom

import Commands.GitLabRepoUrlFinder
import cats.MonadError
import cats.data.EitherT
import cats.data.EitherT.right
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.config.ConfigLoader
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.Commit
import ch.datascience.triplesgenerator.eventprocessing.Commit._
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

class RenkuLogTriplesGenerator private[renkulog] (
    gitRepoUrlFinder:    GitLabRepoUrlFinder[IO],
    renku:               Commands.Renku,
    file:                Commands.File,
    git:                 Commands.Git,
    randomLong:          () => Long
)(implicit contextShift: ContextShift[IO])
    extends TriplesGenerator[IO] {

  import ammonite.ops.{Path, root}
  import file._
  import gitRepoUrlFinder._

  private val workDirectory: Path = root / "tmp"
  private val repositoryDirectoryFinder = ".*/(.*)$".r

  def generateTriples(commit:           Commit,
                      maybeAccessToken: Option[AccessToken]): EitherT[IO, GenerationRecoverableError, JsonLDTriples] =
    EitherT {
      createRepositoryDirectory(commit.project.path)
        .bracket(cloneCheckoutGenerate(commit, maybeAccessToken))(deleteDirectory)
        .recoverWith(meaningfulError(maybeAccessToken))
    }

  private def cloneCheckoutGenerate(
      commit:           Commit,
      maybeAccessToken: Option[AccessToken]
  )(repoDirectory:      Path): IO[Either[GenerationRecoverableError, JsonLDTriples]] = {
    for {
      repositoryUrl <- findRepositoryUrl(commit.project.path, maybeAccessToken).toRight
      _             <- git clone (repositoryUrl, repoDirectory, workDirectory)
      _             <- (git checkout (commit.id, repoDirectory)).toRight
      triples       <- findTriples(commit, repoDirectory).toRight
    } yield triples
  }.value

  private implicit class IOOps[Right](io: IO[Right]) {
    lazy val toRight: EitherT[IO, GenerationRecoverableError, Right] = right[GenerationRecoverableError](io)
  }

  private def createRepositoryDirectory(projectPath: ProjectPath): IO[Path] =
    contextShift.shift *> mkdir(tempDirectoryName(repositoryNameFrom(projectPath)))

  private def tempDirectoryName(repositoryName: String) =
    workDirectory / s"$repositoryName-${randomLong()}"

  private def repositoryNameFrom(projectPath: ProjectPath): String = projectPath.value match {
    case repositoryDirectoryFinder(folderName) => folderName
  }

  private def findTriples(commit: Commit, repositoryDirectory: Path): IO[JsonLDTriples] = {
    import renku._

    commit match {
      case withParent:    CommitWithParent    => renku.log(withParent, repositoryDirectory)
      case withoutParent: CommitWithoutParent => renku.log(withoutParent, repositoryDirectory)
    }
  }

  private def meaningfulError(
      maybeAccessToken: Option[AccessToken]
  ): PartialFunction[Throwable, IO[Either[GenerationRecoverableError, JsonLDTriples]]] = {
    case NonFatal(exception) =>
      IO.raiseError {
        (Option(exception.getMessage) -> maybeAccessToken)
          .mapN { (message, token) =>
            if (message contains token.value)
              new Exception(s"Triples generation failed: ${message replaceAll (token.value, token.toString)}")
            else
              new Exception("Triples generation failed", exception)
          }
          .getOrElse(new Exception("Triples generation failed", exception))
      }
  }
}

object RenkuLogTriplesGenerator {

  def apply()(implicit contextShift: ContextShift[IO],
              executionContext:      ExecutionContext,
              timer:                 Timer[IO]): IO[TriplesGenerator[IO]] =
    for {
      renkuLogTimeout <- new RenkuLogTimeoutConfigProvider[IO].get
      gitLabUrl       <- GitLabUrl[IO]()
    } yield new RenkuLogTriplesGenerator(
      new GitLabRepoUrlFinder[IO](gitLabUrl),
      new Commands.Renku(renkuLogTimeout),
      new Commands.File,
      new Commands.Git,
      randomLong = new SecureRandom().nextLong _
    )
}

private class RenkuLogTimeoutConfigProvider[Interpretation[_]](
    configuration: Config = ConfigFactory.load()
)(implicit ME:     MonadError[Interpretation, Throwable])
    extends ConfigLoader[Interpretation] {

  def get: Interpretation[FiniteDuration] = find[FiniteDuration]("renku-log-timeout", configuration)
}
