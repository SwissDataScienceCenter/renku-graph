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

import cats.MonadError
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.ServiceUrl
import ch.datascience.graph.config.{GitLabUrl, RenkuLogTimeout}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent._
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

private object Commands {

  class GitLabRepoUrlFinder[Interpretation[_]](
      gitLabUrl: GitLabUrl
  )(implicit ME: MonadError[Interpretation, Throwable]) {

    import java.net.URL

    def findRepositoryUrl(projectPath:      projects.Path,
                          maybeAccessToken: Option[AccessToken]
    ): Interpretation[ServiceUrl] =
      merge(gitLabUrl, findUrlTokenPart(maybeAccessToken), projectPath)

    private lazy val findUrlTokenPart: Option[AccessToken] => String = {
      case None                             => ""
      case Some(PersonalAccessToken(token)) => s"gitlab-ci-token:$token@"
      case Some(OAuthAccessToken(token))    => s"oauth2:$token@"
    }

    private def merge(gitLabUrl:    GitLabUrl,
                      urlTokenPart: String,
                      projectPath:  projects.Path
    ): Interpretation[ServiceUrl] = ME.fromEither {
      ServiceUrl.from {
        val url              = gitLabUrl.value
        val protocol         = new URL(url).getProtocol
        val serviceWithToken = url.replace(s"$protocol://", s"$protocol://$urlTokenPart")
        s"$serviceWithToken/$projectPath.git"
      }
    }
  }

  import ammonite.ops
  import ammonite.ops._

  class File {

    def mkdir(newDir: Path): IO[Path] = IO {
      ops.mkdir ! newDir
      newDir
    }

    def deleteDirectory(repositoryDirectory: Path): IO[Unit] = IO {
      ops.rm ! repositoryDirectory
    }
  }

  class Git(
      doClone: (ServiceUrl, Path, Path) => CommandResult = (url, destinationDir, workDir) =>
        %%('git, 'clone, url.toString, destinationDir.toString)(workDir)
  ) {
    import cats.data.EitherT
    import cats.syntax.all._
    import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError

    def checkout(commitId: CommitId, repositoryDirectory: Path): IO[Unit] =
      IO {
        %%('git, 'checkout, commitId.value)(repositoryDirectory)
      }.map(_ => ())

    def findCommitMessage(commitId: CommitId, repositoryDirectory: Path): IO[String] =
      IO {
        %%('git, 'log, "--format=%B", "-n", "1", commitId.value)(repositoryDirectory)
      }.map(_.out.string.trim)

    def clone(
        repositoryUrl:        ServiceUrl,
        destinationDirectory: Path,
        workDirectory:        Path
    ): EitherT[IO, GenerationRecoverableError, Unit] =
      EitherT[IO, GenerationRecoverableError, Unit] {
        IO {
          doClone(repositoryUrl, destinationDirectory, workDirectory)
        }.map(_ => ().asRight[GenerationRecoverableError])
          .recoverWith(relevantError)
      }

    private val recoverableErrors = Set("SSL_ERROR_SYSCALL", "the remote end hung up unexpectedly")
    private lazy val relevantError: PartialFunction[Throwable, IO[Either[GenerationRecoverableError, Unit]]] = {
      case ShelloutException(result) =>
        def errorMessage(message: String) = s"git clone failed with: $message"

        IO(result.out.string) flatMap {
          case out if recoverableErrors exists out.contains =>
            GenerationRecoverableError(errorMessage(result.toString())).asLeft[Unit].pure[IO]
          case _ =>
            new Exception(errorMessage(result.toString())).raiseError[IO, Either[GenerationRecoverableError, Unit]]
        }
    }
  }

  class Renku(
      timeout: RenkuLogTimeout
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      ME:               MonadError[IO, Throwable],
      executionContext: ExecutionContext
  ) {

    import cats.syntax.all._

    import scala.util.Try

    def migrate(commitEvent: CommitEvent, destinationDirectory: Path): IO[Unit] =
      IO(%%('renku, 'migrate)(destinationDirectory))
        .flatMap(_ => IO.unit)
        .recoverWith { case NonFatal(exception) =>
          IO.raiseError {
            new Exception(
              s"'renku migrate' failed for commit: ${commitEvent.commitId}, project: ${commitEvent.project.id}",
              exception
            )
          }
        }

    def log[T <: CommitEvent](
        commit:                 T,
        destinationDirectory:   Path
    )(implicit generateTriples: (T, Path) => CommandResult): EitherT[IO, ProcessingRecoverableError, JsonLDTriples] =
      EitherT {
        IO.race(
          call(generateTriples(commit, destinationDirectory)),
          timer sleep timeout.value
        ).flatMap {
          case Left(result) => IO.pure(result)
          case Right(_)     => timeoutExceededError(commit)
        }
      }

    private def timeoutExceededError(commitEvent: CommitEvent): IO[Either[ProcessingRecoverableError, JsonLDTriples]] =
      ME.raiseError {
        new Exception(
          s"'renku log' execution for commit: ${commitEvent.commitId}, project: ${commitEvent.project.id} took longer than $timeout - terminating"
        )
      }

    private def call(generateTriples: => CommandResult): IO[Either[ProcessingRecoverableError, JsonLDTriples]] =
      IO.cancelable[Either[ProcessingRecoverableError, JsonLDTriples]] { callback =>
        executionContext.execute { () =>
          {
            for {
              triplesAsString <- Try(generateTriples.out.string.trim)
              wrappedTriples  <- JsonLDTriples.parse[Try](triplesAsString)
            } yield callback(Right(Right(wrappedTriples)))
          } recover {
            case NonFatal(ShelloutException(result)) if result.exitCode == 137 =>
              callback(Right(Left(GenerationRecoverableError("Not enough memory"))))
            case NonFatal(exception) =>
              callback(Left(exception))
          } fold (throw _, identity)
        }
        IO.unit
      }

    implicit val commitWithoutParentTriplesFinder: (CommitEventWithoutParent, Path) => CommandResult = {
      case (_, destinationDirectory) =>
        %%('renku, 'log, "--format", "json-ld", "--strict")(destinationDirectory)
    }

    implicit val commitWithParentTriplesFinder: (CommitEventWithParent, Path) => CommandResult = {
      case (commit, destinationDirectory) =>
        val changedFiles = %%(
          'git,
          "diff-tree",
          "--no-commit-id",
          "--name-only",
          "-r",
          commit.commitId.toString
        )(destinationDirectory).out.lines

        %%(
          'renku,
          'log,
          "--format",
          "json-ld",
          "--strict",
          "--revision",
          s"${commit.parentId}^..${commit.commitId}",
          changedFiles
        )(destinationDirectory)
    }
  }
}
