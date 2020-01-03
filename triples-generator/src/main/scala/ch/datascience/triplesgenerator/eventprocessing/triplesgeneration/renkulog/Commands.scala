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
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.ServiceUrl
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.Commit
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.control.NonFatal

private object Commands {

  class GitLabRepoUrlFinder[Interpretation[_]](
      gitLabUrl: GitLabUrl
  )(implicit ME: MonadError[Interpretation, Throwable]) {

    import java.net.URL

    def findRepositoryUrl(projectPath: ProjectPath, maybeAccessToken: Option[AccessToken]): Interpretation[ServiceUrl] =
      merge(gitLabUrl, findUrlTokenPart(maybeAccessToken), projectPath)

    private lazy val findUrlTokenPart: Option[AccessToken] => String = {
      case None                             => ""
      case Some(PersonalAccessToken(token)) => s"gitlab-ci-token:$token@"
      case Some(OAuthAccessToken(token))    => s"oauth2:$token@"
    }

    private def merge(gitLabUrl:    GitLabUrl,
                      urlTokenPart: String,
                      projectPath:  ProjectPath): Interpretation[ServiceUrl] = ME.fromEither {
      ServiceUrl.from {
        val url              = gitLabUrl.value
        val protocol         = new URL(url).getProtocol
        val serviceWithToken = url.toString.replace(s"$protocol://", s"$protocol://$urlTokenPart")
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
    import cats.data.EitherT._
    import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError

    def checkout(
        commitId:            CommitId,
        repositoryDirectory: Path
    ): IO[CommandResult] = IO {
      %%('git, 'checkout, commitId.value)(repositoryDirectory)
    }

    def clone(
        repositoryUrl:        ServiceUrl,
        destinationDirectory: Path,
        workDirectory:        Path
    ): EitherT[IO, GenerationRecoverableError, CommandResult] =
      for {
        result <- doClone(repositoryUrl, destinationDirectory, workDirectory).toRightT
        errorOrResult <- if (result.exitCode == 0) result.toRightT
                        else sslErrorToLeft(result)

      } yield errorOrResult

    private def sslErrorToLeft(result: CommandResult) =
      for {
        message <- result.out.string.toRightT
        errorOrResult <- if (message contains "SSL_ERROR_SYSCALL") GenerationRecoverableError(message).toLeftT
                        else message.raiseError
      } yield errorOrResult

    private implicit class CommandResultOps(value: CommandResult) {
      lazy val toRightT: EitherT[IO, GenerationRecoverableError, CommandResult] =
        rightT[IO, GenerationRecoverableError](value)
    }

    private implicit class ErrorOps(value: GenerationRecoverableError) {
      lazy val toLeftT: EitherT[IO, GenerationRecoverableError, CommandResult] = leftT[IO, CommandResult](value)
    }

    private implicit class StringOps(value: String) {
      lazy val toRightT: EitherT[IO, GenerationRecoverableError, String] = rightT[IO, GenerationRecoverableError](value)
      lazy val raiseError: EitherT[IO, GenerationRecoverableError, CommandResult] =
        EitherT[IO, GenerationRecoverableError, CommandResult](IO.raiseError(new Exception(value)))
    }
  }

  class Renku(
      timeout:             FiniteDuration
  )(implicit contextShift: ContextShift[IO],
    timer:                 Timer[IO],
    ME:                    MonadError[IO, Throwable],
    executionContext:      ExecutionContext) {

    import cats.implicits._

    import scala.util.Try

    def log[T <: Commit](
        commit:                 T,
        destinationDirectory:   Path
    )(implicit generateTriples: (T, Path) => CommandResult): IO[JsonLDTriples] =
      IO.race(
          call(generateTriples(commit, destinationDirectory)),
          timer.sleep(timeout)
        )
        .flatMap {
          case Left(triples) => IO.pure(triples)
          case Right(_)      => timeoutExceededError(commit)
        }

    private def timeoutExceededError(commit: Commit): IO[JsonLDTriples] = ME.raiseError {
      new Exception(
        s"'renku log' execution for commit: ${commit.id}, project: ${commit.project.id} took longer than $timeout - terminating"
      )
    }

    private def call(generateTriples: => CommandResult) =
      IO.cancelable[JsonLDTriples] { callback =>
        executionContext.execute { () =>
          {
            for {
              triplesAsString <- Try(generateTriples.out.string.trim)
              wrappedTriples  <- JsonLDTriples.parse[Try](triplesAsString)
            } yield callback(Right(wrappedTriples))
          }.recover { case NonFatal(exception) => callback(Left(exception)) }
            .fold(throw _, identity)
        }
        IO.unit
      }

    implicit val commitWithoutParentTriplesFinder: (CommitWithoutParent, Path) => CommandResult = {
      case (_, destinationDirectory) =>
        %%('renku, 'log, "--format", "json-ld")(destinationDirectory)
    }

    implicit val commitWithParentTriplesFinder: (CommitWithParent, Path) => CommandResult = {
      case (commit, destinationDirectory) =>
        val changedFiles = %%(
          'git,
          'diff,
          "--name-only",
          "--diff-filter=d",
          s"${commit.parentId}..${commit.id}"
        )(destinationDirectory).out.lines

        %%(
          'renku,
          'log,
          "--format",
          "json-ld",
          "--revision",
          s"${commit.parentId}..${commit.id}",
          changedFiles
        )(destinationDirectory)
    }
  }
}
