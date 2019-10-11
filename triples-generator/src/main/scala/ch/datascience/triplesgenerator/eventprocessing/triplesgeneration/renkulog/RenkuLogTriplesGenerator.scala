/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

  def generateTriples(commit: Commit, maybeAccessToken: Option[AccessToken]): IO[JsonLDTriples] =
    createRepositoryDirectory(commit.project.path)
      .bracket { repositoryDirectory =>
        for {
          gitRepositoryUrl <- findRepositoryUrl(commit.project.path, maybeAccessToken)
          _                <- git cloneRepo (gitRepositoryUrl, repositoryDirectory, workDirectory)
          _                <- git checkout (commit.id, repositoryDirectory)
          triples          <- findTriples(commit, repositoryDirectory)
        } yield triples
      }(repositoryDirectory => delete(repositoryDirectory))
      .recoverWith(meaningfulError)

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

  private lazy val meaningfulError: PartialFunction[Throwable, IO[JsonLDTriples]] = {
    case NonFatal(exception) =>
      IO.raiseError(new RuntimeException("Triples generation failed", exception))
  }
}

object RenkuLogTriplesGenerator {

  def apply()(implicit contextShift: ContextShift[IO],
              executionContext:      ExecutionContext,
              timer:                 Timer[IO]): IO[TriplesGenerator[IO]] =
    for {
      renkuLogTimeout <- new RenkuLogTimeoutConfigProvider[IO].get
      gitLabUrl       <- GitLabUrl[IO]()
    } yield
      new RenkuLogTriplesGenerator(
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
