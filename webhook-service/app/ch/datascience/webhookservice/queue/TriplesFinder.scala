/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.queue

import java.io.{ ByteArrayInputStream, InputStream }
import java.security.SecureRandom

import cats.implicits._
import ch.datascience.webhookservice.{ CheckoutSha, GitRepositoryUrl }
import javax.inject.{ Inject, Singleton }
import org.apache.jena.rdf.model.ModelFactory

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions
import scala.util.Try
import scala.util.control.NonFatal

@Singleton
private class TriplesFinder(
    file:         Commands.File,
    git:          Commands.Git,
    renku:        Commands.Renku,
    toRdfTriples: InputStream => RDFTriples,
    randomLong:   () => Long
) {

  @Inject() def this(
      file:  Commands.File,
      git:   Commands.Git,
      renku: Commands.Renku
  ) = this(
    file,
    git,
    renku,
    ( inputStream: InputStream ) => RDFTriples( ModelFactory.createDefaultModel.read( inputStream, "" ) ),
    new SecureRandom().nextLong _
  )

  import ammonite.ops.{ Path, root }
  import file._

  private val workDirectory: Path = root / "tmp"
  private val repositoryDirectoryFinder = ".*/(.*).git$".r

  def generateTriples(
      gitRepositoryUrl: GitRepositoryUrl,
      checkoutSha:      CheckoutSha
  )( implicit executionContext: ExecutionContext ): Future[Either[Throwable, RDFTriples]] = Future {

    val repositoryDirectory = tempDirectoryName( repositoryNameFrom( gitRepositoryUrl ) )

    val maybeTriplesFile = for {
      _ <- pure( mkdir( repositoryDirectory ) )
      _ <- pure( git cloneRepo ( gitRepositoryUrl, repositoryDirectory, workDirectory ) )
      _ <- pure( git checkout ( checkoutSha, repositoryDirectory ) )
      triplesStream <- pure( renku log repositoryDirectory )
      rdfTriples <- pure( toRdfTriples( triplesStream ) )
      _ <- pure( removeSilently( repositoryDirectory ) )
    } yield rdfTriples

    maybeTriplesFile.toEither.leftMap {
      case NonFatal( exception ) =>
        removeSilently( repositoryDirectory )
        exception
      case other => throw other
    }
  }

  private def tempDirectoryName( repositoryName: String ) =
    workDirectory / s"$repositoryName-${randomLong()}"

  private def repositoryNameFrom( gitRepositoryUrl: GitRepositoryUrl ): String = gitRepositoryUrl.value match {
    case repositoryDirectoryFinder( folderName ) => folderName
  }

  private implicit def pure[V]( maybeValue: => V ): Try[V] =
    Try( maybeValue )
}

private object Commands {

  import ammonite.ops
  import ammonite.ops._

  @Singleton
  class File {

    def mkdir( newDir: Path ): Unit = ops.mkdir ! newDir

    def removeSilently( path: java.nio.file.Path ): Unit =
      removeSilently( Path( path ) )

    def removeSilently( repositoryDirectory: Path ): Unit = Try {
      ops.rm ! repositoryDirectory
    } fold (
      _ => (),
      identity
    )
  }

  @Singleton
  class Git {

    def cloneRepo(
        repositoryUrl:        GitRepositoryUrl,
        destinationDirectory: Path,
        workDirectory:        Path
    ): CommandResult =
      %%( 'git, 'clone, repositoryUrl.value, destinationDirectory.toString )( workDirectory )

    def checkout(
        sha:                 CheckoutSha,
        repositoryDirectory: Path
    ): CommandResult =
      %%( 'git, 'checkout, sha.value )( repositoryDirectory )
  }

  @Singleton
  class Renku {

    def log( destinationDirectory: Path ): InputStream =
      %%( 'renku, 'log, "--format", "rdf" )( destinationDirectory ).out.toInputStream

    implicit private class StreamValueOps( streamValue: StreamValue ) {
      lazy val toInputStream: InputStream =
        new ByteArrayInputStream( streamValue.chunks.flatMap( _.array ).toArray )
    }
  }
}
