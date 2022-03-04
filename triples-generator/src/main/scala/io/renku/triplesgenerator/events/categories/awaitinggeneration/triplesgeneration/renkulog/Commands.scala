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

package io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog

import ammonite.ops.Path
import cats.MonadThrow
import cats.data.EitherT
import cats.effect.kernel.Async
import cats.syntax.all._
import io.renku.config.ServiceUrl
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.parser._
import io.renku.tinytypes.{TinyType, TinyTypeFactory}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.categories.awaitinggeneration.CommitEvent
import io.renku.triplesgenerator.events.categories.{ProcessingNonRecoverableError, ProcessingRecoverableError}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private object Commands {

  trait AbsolutePathTinyType extends Any with TinyType {
    type V = Path
  }

  final class RepositoryPath private (val value: Path) extends AnyVal with AbsolutePathTinyType

  object RepositoryPath extends TinyTypeFactory[RepositoryPath](new RepositoryPath(_))

  trait GitLabRepoUrlFinder[F[_]] {
    def findRepositoryUrl(projectPath: projects.Path)(implicit maybeAccessToken: Option[AccessToken]): F[ServiceUrl]
  }

  class GitLabRepoUrlFinderImpl[F[_]: MonadThrow](gitLabUrl: GitLabUrl) extends GitLabRepoUrlFinder[F] {

    import java.net.URL

    override def findRepositoryUrl(projectPath: projects.Path)(implicit
        maybeAccessToken:                       Option[AccessToken]
    ): F[ServiceUrl] =
      merge(gitLabUrl, findUrlTokenPart(maybeAccessToken), projectPath)

    private lazy val findUrlTokenPart: Option[AccessToken] => String = {
      case None                             => ""
      case Some(PersonalAccessToken(token)) => s"gitlab-ci-token:$token@"
      case Some(OAuthAccessToken(token))    => s"oauth2:$token@"
    }

    private def merge(gitLabUrl: GitLabUrl, urlTokenPart: String, projectPath: projects.Path): F[ServiceUrl] =
      MonadThrow[F].fromEither {
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

  trait File[F[_]] {
    def mkdir(newDir:                        Path): F[Path]
    def deleteDirectory(repositoryDirectory: Path): F[Unit]
    def exists(fileName:                     Path): F[Boolean]
  }

  object File {
    def apply[F[_]: MonadThrow: Logger]: File[F] = new FileImpl[F]
  }

  class FileImpl[F[_]: MonadThrow: Logger] extends File[F] {

    override def mkdir(newDir: Path): F[Path] = MonadThrow[F].catchNonFatal {
      ops.mkdir ! newDir
      newDir
    }

    override def deleteDirectory(repositoryDirectory: Path): F[Unit] = MonadThrow[F]
      .catchNonFatal {
        ops.rm ! repositoryDirectory
      }
      .recoverWith { case NonFatal(ex) =>
        Logger[F].error(ex)("Error when deleting repo directory")
      }

    override def exists(fileName: Path): F[Boolean] = MonadThrow[F].catchNonFatal {
      ops.exists(fileName)
    }
  }

  trait Git[F[_]] {
    def checkout(commitId:                           CommitId)(implicit repositoryDirectory: RepositoryPath): F[Unit]
    def `reset --hard`(implicit repositoryDirectory: RepositoryPath): F[Unit]
    def `rm --cached`(implicit repositoryDirectory:  RepositoryPath): F[Unit]
    def `add -A`(implicit repositoryDirectory:       RepositoryPath): F[Unit]
    def commit(message:                              String)(implicit repositoryDirectory:   RepositoryPath): F[Unit]
    def clone(
        repositoryUrl:                       ServiceUrl,
        workDirectory:                       Path
    )(implicit destinationDirectory:         RepositoryPath): EitherT[F, ProcessingRecoverableError, Unit]
    def rm(fileName:                         Path)(implicit repositoryDirectory: RepositoryPath): F[Unit]
    def status(implicit repositoryDirectory: RepositoryPath): F[String]
  }

  object Git {
    def apply[F[_]: MonadThrow]: Git[F] = new GitImpl[F]()
  }

  class GitImpl[F[_]: MonadThrow](
      doClone: (ServiceUrl, RepositoryPath, Path) => CommandResult = (url, destinationDir, workDir) =>
        %%("git", "clone", "--no-checkout", url.toString, destinationDir.toString)(workDir)
  ) extends Git[F] {
    import cats.data.EitherT
    import cats.syntax.all._

    override def checkout(commitId: CommitId)(implicit repositoryDirectory: RepositoryPath): F[Unit] =
      MonadThrow[F].catchNonFatal {
        %%("git", "checkout", commitId.value)(repositoryDirectory.value)
      }.void

    override def `reset --hard`(implicit repositoryDirectory: RepositoryPath): F[Unit] = MonadThrow[F].catchNonFatal {
      %%("git", "reset", "--hard")(repositoryDirectory.value)
    }.void

    override def `rm --cached`(implicit repositoryDirectory: RepositoryPath): F[Unit] = MonadThrow[F].catchNonFatal {
      %%("git", "rm", "--cached", ".")(repositoryDirectory.value)
    }.void

    override def `add -A`(implicit repositoryDirectory: RepositoryPath): F[Unit] = MonadThrow[F].catchNonFatal {
      %%("git", "add", "-A")(repositoryDirectory.value)
    }.void

    override def commit(message: String)(implicit repoDir: RepositoryPath): F[Unit] = MonadThrow[F].catchNonFatal {
      %%("git", "commit", "-m", message)(repoDir.value)
    }.void

    override def clone(
        repositoryUrl:               ServiceUrl,
        workDirectory:               Path
    )(implicit destinationDirectory: RepositoryPath): EitherT[F, ProcessingRecoverableError, Unit] =
      EitherT[F, ProcessingRecoverableError, Unit] {
        MonadThrow[F]
          .catchNonFatal {
            doClone(repositoryUrl, destinationDirectory, workDirectory)
          }
          .map(_ => ().asRight[ProcessingRecoverableError])
          .recoverWith(relevantError)
      }

    override def rm(fileName: Path)(implicit repositoryDirectory: RepositoryPath): F[Unit] =
      MonadThrow[F].catchNonFatal {
        %%("git", "rm", fileName)(repositoryDirectory.value)
      }.void

    override def status(implicit repositoryDirectory: RepositoryPath): F[String] = MonadThrow[F].catchNonFatal {
      %%("git", "status")(repositoryDirectory.value).out.string
    }

    private val authRecoverableErrors = Set(
      "fatal: Authentication failed for",
      "fatal: could not read Username for"
    )
    private val nonAuthRecoverableErrors = Set(
      "SSL_ERROR_SYSCALL",
      "The requested URL returned error: 502",
      "The requested URL returned error: 503",
      "Could not resolve host:",
      "Host is unreachable"
    )
    private val malformedRepositoryErrors = Set(
      "the remote end hung up unexpectedly",
      "The requested URL returned error: 504",
      "Error in the HTTP2 framing layer"
    )
    private lazy val relevantError: PartialFunction[Throwable, F[Either[ProcessingRecoverableError, Unit]]] = {
      case ShelloutException(result) =>
        def errorMessage(message: String) = s"git clone failed with: $message"

        MonadThrow[F].catchNonFatal(result.toString()) >>= {
          case out if authRecoverableErrors exists out.contains =>
            AuthRecoverableError(errorMessage(result.toString()))
              .asLeft[Unit]
              .leftWiden[ProcessingRecoverableError]
              .pure[F]
          case out if nonAuthRecoverableErrors exists out.contains =>
            LogWorthyRecoverableError(errorMessage(result.toString()))
              .asLeft[Unit]
              .leftWiden[ProcessingRecoverableError]
              .pure[F]
          case out if malformedRepositoryErrors exists out.contains =>
            ProcessingNonRecoverableError
              .MalformedRepository(errorMessage(result.toString()))
              .raiseError[F, Either[ProcessingRecoverableError, Unit]]
          case _ =>
            new Exception(errorMessage(result.toString())).raiseError[F, Either[ProcessingRecoverableError, Unit]]
        }
    }
  }

  trait Renku[F[_]] {
    def migrate(commitEvent:            CommitEvent)(implicit destinationDirectory: RepositoryPath): F[Unit]
    def graphExport(implicit directory: RepositoryPath): EitherT[F, ProcessingRecoverableError, JsonLD]
  }

  object Renku {
    def apply[F[_]: Async]: Renku[F] = new RenkuImpl[F]()
  }

  class RenkuImpl[F[_]: Async](
      renkuMigrate: Path => CommandResult = %%("renku", "migrate", "--preserve-identifiers")(_),
      renkuExport:  Path => CommandResult = %%("renku", "graph", "export", "--full", "--strict")(_)
  ) extends Renku[F] {

    import cats.syntax.all._

    override def migrate(commitEvent: CommitEvent)(implicit directory: RepositoryPath): F[Unit] =
      MonadThrow[F]
        .catchNonFatal(renkuMigrate(directory.value))
        .void
        .recoverWith { case NonFatal(exception) =>
          new ProcessingNonRecoverableError.MalformedRepository(
            s"'renku migrate' failed for commit: ${commitEvent.commitId}, project: ${commitEvent.project.id}",
            exception
          ).raiseError[F, Unit]
        }

    override def graphExport(implicit directory: RepositoryPath): EitherT[F, ProcessingRecoverableError, JsonLD] =
      EitherT {
        {
          for {
            triplesAsString <- MonadThrow[F].catchNonFatal(renkuExport(directory.value).out.string.trim)
            wrappedTriples  <- MonadThrow[F].fromEither(parse(triplesAsString))
          } yield wrappedTriples.asRight[ProcessingRecoverableError]
        } recoverWith {
          case ShelloutException(result) if result.exitCode == 137 =>
            LogWorthyRecoverableError("Not enough memory").asLeft[JsonLD].leftWiden[ProcessingRecoverableError].pure[F]
          case NonFatal(exception) =>
            ProcessingNonRecoverableError
              .MalformedRepository("'renku graph export' failed", exception)
              .raiseError[F, Either[ProcessingRecoverableError, JsonLD]]
        }
      }
  }
}
