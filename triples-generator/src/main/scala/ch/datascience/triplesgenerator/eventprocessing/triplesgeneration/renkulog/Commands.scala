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

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.ServiceUrl
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.triplesgenerator.eventprocessing.{Commit, RDFTriples}
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

    def delete(repositoryDirectory: Path): IO[Unit] = IO {
      ops.rm ! repositoryDirectory
    }
  }

  class Git {

    def cloneRepo(
        repositoryUrl:        ServiceUrl,
        destinationDirectory: Path,
        workDirectory:        Path
    ): IO[CommandResult] = IO {
      %%('git, 'clone, repositoryUrl.toString, destinationDirectory.toString)(workDirectory)
    }

    def checkout(
        commitId:            CommitId,
        repositoryDirectory: Path
    ): IO[CommandResult] = IO {
      %%('git, 'checkout, commitId.value)(repositoryDirectory)
    }
  }

  class Renku(
      timeout:             FiniteDuration
  )(implicit contextShift: ContextShift[IO],
    timer:                 Timer[IO],
    ME:                    MonadError[IO, Throwable],
    executionContext:      ExecutionContext) {

    import scala.util.Try

    def log[T <: Commit](
        commit:                 T,
        destinationDirectory:   Path
    )(implicit generateTriples: (T, Path) => CommandResult): IO[RDFTriples] =
      IO.race(
          call(generateTriples(commit, destinationDirectory)),
          timer.sleep(timeout)
        )
        .flatMap {
          case Left(rdfTriples) => IO.pure(rdfTriples)
          case Right(_)         => timeoutExceededError(commit)
        }

    private def timeoutExceededError(commit: Commit): IO[RDFTriples] = ME.raiseError {
      new Exception(
        s"'renku log' execution for commit: ${commit.id}, project: ${commit.project.id} took longer than $timeout - terminating"
      )
    }

    private def call(generateTriples: => CommandResult) =
      IO.cancelable[RDFTriples] { callback =>
        executionContext.execute { () =>
          {
            for {
              triplesAsString <- Try(generateTriples.out.string.trim)
              wrappedTriples  <- RDFTriples.from(triplesAsString).toTry
            } yield callback(Right(wrappedTriples))
          }.recover { case NonFatal(exception) => callback(Left(exception)) }
            .fold(throw _, identity)
        }
        IO.unit
      }

    implicit val commitWithoutParentTriplesFinder: (CommitWithoutParent, Path) => CommandResult = {
      case (_, destinationDirectory) =>
        %%('renku, 'log, "--format", "rdf")(destinationDirectory)
    }

    implicit val commitWithParentTriplesFinder: (CommitWithParent, Path) => CommandResult = {
      case (commit, destinationDirectory) =>
        val changedFiles = %%(
          'git,
          'diff,
          "--name-only",
          s"${commit.parentId}..${commit.id}"
        )(destinationDirectory).out.lines

        %%(
          'renku,
          'log,
          "--format",
          "rdf",
          "--revision",
          s"${commit.parentId}..${commit.id}",
          changedFiles
        )(destinationDirectory)
    }
  }
}
