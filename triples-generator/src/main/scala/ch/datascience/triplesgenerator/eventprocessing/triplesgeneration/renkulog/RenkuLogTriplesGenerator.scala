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

import cats.data.EitherT
import cats.data.EitherT.right
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.graph.config.{GitLabUrl, RenkuLogTimeout}
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent._
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog.Commands.GitLabRepoUrlFinder
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

private[eventprocessing] class RenkuLogTriplesGenerator private[renkulog] (
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

  override def generateTriples(
      commitEvent:             CommitEvent
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[IO, ProcessingRecoverableError, JsonLDTriples] =
    EitherT {
      createRepositoryDirectory(commitEvent.project.path)
        .bracket(cloneCheckoutGenerate(commitEvent, maybeAccessToken))(deleteDirectory)
        .recoverWith(meaningfulError(maybeAccessToken))
    }

  private def cloneCheckoutGenerate(
      commitEvent:      CommitEvent,
      maybeAccessToken: Option[AccessToken]
  )(repoDirectory:      Path): IO[Either[ProcessingRecoverableError, JsonLDTriples]] = {
    for {
      repositoryUrl <- findRepositoryUrl(commitEvent.project.path, maybeAccessToken).toRight
      _             <- git clone (repositoryUrl, repoDirectory, workDirectory)
      _             <- (git checkout (commitEvent.commitId, repoDirectory)).toRight
      _             <- (renku migrate (commitEvent, repoDirectory)).toRight
      triples       <- findTriples(commitEvent, repoDirectory).toRight
    } yield triples
  }.value

  private implicit class IOOps[Right](io: IO[Right]) {
    lazy val toRight: EitherT[IO, GenerationRecoverableError, Right] = right[GenerationRecoverableError](io)
  }

  private def createRepositoryDirectory(projectPath: projects.Path): IO[Path] =
    mkdir(tempDirectoryName(repositoryNameFrom(projectPath)))

  private def tempDirectoryName(repositoryName: String) =
    workDirectory / s"$repositoryName-${randomLong()}"

  private def repositoryNameFrom(projectPath: projects.Path): String = projectPath.value match {
    case repositoryDirectoryFinder(folderName) => folderName
  }

  private def findTriples(commitEvent: CommitEvent, repositoryDirectory: Path): IO[JsonLDTriples] = {
    import renku._

    commitEvent match {
      case withParent:    CommitEventWithParent    => renku.log(withParent, repositoryDirectory)
      case withoutParent: CommitEventWithoutParent => renku.log(withoutParent, repositoryDirectory)
    }
  }

  private def meaningfulError(
      maybeAccessToken: Option[AccessToken]
  ): PartialFunction[Throwable, IO[Either[ProcessingRecoverableError, JsonLDTriples]]] = {
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

private[eventprocessing] object RenkuLogTriplesGenerator {

  def apply(logger:     Logger[IO])(implicit contextShift: ContextShift[IO],
              executionContext:      ExecutionContext,
              timer:                 Timer[IO],

  ): IO[TriplesGenerator[IO]] =
    for {
      renkuLogTimeout <- RenkuLogTimeout[IO]()
      gitLabUrl       <- GitLabUrl[IO]()
    } yield new RenkuLogTriplesGenerator(
      new GitLabRepoUrlFinder[IO](gitLabUrl),
      new Commands.Renku(renkuLogTimeout),
      new Commands.File,
      new Commands.Git(logger= logger),
      randomLong = new SecureRandom().nextLong _
    )
}
