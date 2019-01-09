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

package ch.datascience.triplesgenerator.queues.logevent

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URL
import java.security.SecureRandom

import cats.implicits._
import ch.datascience.config.ServiceUrl
import ch.datascience.graph.events.{CommitId, ProjectPath}
import ch.datascience.triplesgenerator.queues.logevent.LogEventQueue.{Commit, CommitWithParent, CommitWithoutParent}
import javax.inject.{Inject, Named, Singleton}
import org.apache.jena.rdf.model.ModelFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Try
import scala.util.control.NonFatal

@Singleton
private class TriplesFinder(
    file:         Commands.File,
    git:          Commands.Git,
    renku:        Commands.Renku,
    gitLabUrl:    ServiceUrl,
    toRdfTriples: InputStream => RDFTriples,
    randomLong:   () => Long
) {

  @Inject() def this(
      file:                          Commands.File,
      git:                           Commands.Git,
      renku:                         Commands.Renku,
      @Named("gitlabUrl") gitlabUrl: URL
  ) = this(
    file,
    git,
    renku,
    ServiceUrl(gitlabUrl),
    (inputStream: InputStream) => RDFTriples(ModelFactory.createDefaultModel.read(inputStream, "")),
    new SecureRandom().nextLong _
  )

  import ammonite.ops.{Path, root}
  import file._

  private val workDirectory: Path = root / "tmp"
  private val repositoryDirectoryFinder = ".*/(.*)$".r

  def generateTriples(commit:    Commit)(
      implicit executionContext: ExecutionContext): Future[Either[Throwable, RDFTriples]] = Future {
    val repositoryDirectory = tempDirectoryName(repositoryNameFrom(commit.projectPath))
    val gitRepositoryUrl    = gitLabUrl / s"${commit.projectPath}.git"

    val maybeTriplesFile = for {
      _             <- pure(mkdir(repositoryDirectory))
      _             <- git cloneRepo (gitRepositoryUrl, repositoryDirectory, workDirectory)
      _             <- git checkout (commit.id, repositoryDirectory)
      triplesStream <- findTriplesStream(commit, repositoryDirectory)
      rdfTriples    <- toRdfTriples(triplesStream)
      _             <- removeSilently(repositoryDirectory)
    } yield rdfTriples

    maybeTriplesFile.toEither.leftMap {
      case NonFatal(exception) =>
        removeSilently(repositoryDirectory)
        exception
      case other => throw other
    }
  }

  private def tempDirectoryName(repositoryName: String) =
    workDirectory / s"$repositoryName-${randomLong()}"

  private def repositoryNameFrom(projectPath: ProjectPath): String = projectPath.value match {
    case repositoryDirectoryFinder(folderName) => folderName
  }

  private def findTriplesStream(commit: Commit, repositoryDirectory: Path): InputStream = {
    import renku._

    commit match {
      case withParent:    CommitWithParent    => renku.log(withParent, repositoryDirectory)
      case withoutParent: CommitWithoutParent => renku.log(withoutParent, repositoryDirectory)
    }
  }

  private implicit def pure[V](maybeValue: => V): Try[V] =
    Try(maybeValue)
}

private object Commands {

  import ammonite.ops
  import ammonite.ops._

  @Singleton
  class File {

    def mkdir(newDir: Path): Unit = ops.mkdir ! newDir

    def removeSilently(path: java.nio.file.Path): Unit =
      removeSilently(Path(path))

    def removeSilently(repositoryDirectory: Path): Unit =
      Try {
        ops.rm ! repositoryDirectory
      } fold (
        _ => (),
        identity
      )
  }

  @Singleton
  class Git {

    def cloneRepo(
        repositoryUrl:        ServiceUrl,
        destinationDirectory: Path,
        workDirectory:        Path
    ): CommandResult =
      %%('git, 'clone, repositoryUrl.toString, destinationDirectory.toString)(workDirectory)

    def checkout(
        commitId:            CommitId,
        repositoryDirectory: Path
    ): CommandResult =
      %%('git, 'checkout, commitId.value)(repositoryDirectory)
  }

  @Singleton
  class Renku {

    def log[T <: Commit](commit:  T, destinationDirectory: Path)(
        implicit generateTriples: (T, Path) => CommandResult): InputStream =
      generateTriples(commit, destinationDirectory).out.toInputStream

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
        )(destinationDirectory).out.lines.mkString("\n")

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

    private implicit class StreamValueOps(streamValue: StreamValue) {
      lazy val toInputStream: InputStream =
        new ByteArrayInputStream(streamValue.chunks.flatMap(_.array).toArray)
    }
  }
}
