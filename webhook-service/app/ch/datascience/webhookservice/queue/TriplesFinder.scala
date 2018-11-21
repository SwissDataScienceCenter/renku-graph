package ch.datascience.webhookservice.queue

import java.io.{ ByteArrayInputStream, InputStream }
import java.nio.file.Files.copy
import java.security.SecureRandom

import ch.datascience.webhookservice.{ CheckoutSha, GitRepositoryUrl, queue }
import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions
import scala.util.Try

@Singleton
private class TriplesFinder(
    file:       Commands.File,
    git:        Commands.Git,
    renku:      Commands.Renku,
    randomLong: () => Long
) {

  @Inject() def this(
      file:  Commands.File,
      git:   Commands.Git,
      renku: Commands.Renku
  ) = this( file, git, renku, new SecureRandom().nextLong _ )

  import ammonite.ops.{ Path, root }
  import file._

  private val workDirectory: Path = root / "tmp"
  private val repositoryDirectoryFinder = ".*/(.*).git$".r

  def generateTriples(
      gitRepositoryUrl: GitRepositoryUrl,
      checkoutSha:      CheckoutSha
  )( implicit executionContext: ExecutionContext ): Future[Either[Throwable, TriplesFile]] = Future {

    val pathDifferentiator = randomLong()
    val repositoryName = extractRepositoryName( gitRepositoryUrl )
    val repositoryDirectory = tempDirectoryName( repositoryName, pathDifferentiator )
    val triplesFile = composeTriplesFileName( repositoryName, pathDifferentiator )

    val maybeTriplesFile = for {
      _ <- pure( mkdir( repositoryDirectory ) )
      _ <- pure( git cloneRepo ( gitRepositoryUrl, repositoryDirectory, workDirectory ) )
      _ <- pure( git checkout ( checkoutSha, repositoryDirectory ) )
      _ <- pure( renku.logToFile( triplesFile, repositoryDirectory ) )
      _ <- pure( removeSilently( repositoryDirectory ) )
    } yield triplesFile

    maybeTriplesFile.fold(
      exception => {
        removeSilently( repositoryDirectory )
        Left( exception )
      },
      triplesFile => Right( triplesFile )
    )
  }

  private def extractRepositoryName( gitRepositoryUrl: GitRepositoryUrl ): String = gitRepositoryUrl.value match {
    case repositoryDirectoryFinder( folderName ) => folderName
  }

  private def tempDirectoryName( repositoryName: String, pathDifferentiator: Long ) =
    workDirectory / s"$repositoryName-$pathDifferentiator"

  private def composeTriplesFileName( repositoryName: String, pathDifferentiator: Long ) =
    queue.TriplesFile( ( workDirectory / s"$repositoryName-$pathDifferentiator.rdf" ).toNIO )

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

    def logToFile( triplesFile: TriplesFile, destinationDirectory: Path ): Unit = {
      val triplesStream = %%( 'renku, 'log, "--format", "rdf" )( destinationDirectory ).out.toInputStream
      copy( triplesStream, triplesFile.value )
    }

    implicit private class StreamValueOps( streamValue: StreamValue ) {
      lazy val toInputStream: InputStream =
        new ByteArrayInputStream( streamValue.chunks.flatMap( _.array ).toArray )
    }
  }
}
