package ch.datascience.webhookservice.queue

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.file.Files.copy
import java.security.SecureRandom

import ch.datascience.webhookservice.queue.Commands.{File, Git, Renku}
import ch.datascience.webhookservice.{CheckoutSha, GitRepositoryUrl}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Try

private class TriplesFinder(file: Commands.File,
                            git: Commands.Git,
                            renku: Commands.Renku,
                            randomLong: () => Long) {

  import TriplesFinder._
  import file._

  def generateTriples(gitRepositoryUrl: GitRepositoryUrl,
                      checkoutSha: CheckoutSha)
                     (implicit executionContext: ExecutionContext): Future[Either[Throwable, TriplesFile]] = Future {

    val repositoryDirectory = tempDirectoryName(repositoryDirName(gitRepositoryUrl))

    val maybeTriplesFile = for {
      _ <- pure(mkdir(repositoryDirectory))
      _ <- pure(git cloneRepo(gitRepositoryUrl, repositoryDirectory, workDirectory))
      _ <- pure(git checkout(checkoutSha, repositoryDirectory))
      triplesFile <- pure(renku logToFile repositoryDirectory)
    } yield triplesFile

    maybeTriplesFile.fold(
      exception => {
        removeSilently(repositoryDirectory)
        Left(exception)
      },
      triplesFile => Right(triplesFile)
    )
  }

  private def tempDirectoryName(repositoryName: String) =
    workDirectory / s"$repositoryName-${randomLong()}"

  private def repositoryDirName(gitRepositoryUrl: GitRepositoryUrl): String = gitRepositoryUrl.value match {
    case repositoryDirectoryFinder(folderName) => folderName
  }

  private implicit def pure[V](maybeValue: => V): Try[V] =
    Try(maybeValue)
}

private object TriplesFinder {

  import ammonite.ops.{Path, root}

  private val workDirectory: Path = root / "tmp"
  private val repositoryDirectoryFinder = ".*/(.*).git$".r

  def apply(): TriplesFinder = new TriplesFinder(
    file = new File(),
    git = new Git(),
    renku = new Renku(),
    randomLong = new SecureRandom().nextLong
  )
}

private object Commands {

  import ammonite.ops
  import ammonite.ops._

  class File {


    def mkdir(newDir: Path): Unit = ops.mkdir ! newDir

    def removeSilently(repositoryDirectory: Path): Unit = Try {
      ops.rm ! repositoryDirectory
    } fold(
      _ => (),
      identity
    )
  }

  class Git {

    def cloneRepo(repositoryUrl: GitRepositoryUrl,
                  destinationDirectory: Path,
                  workDirectory: Path): CommandResult =
      %%('git, 'clone, repositoryUrl.value, destinationDirectory.toString)(workDirectory)

    def checkout(sha: CheckoutSha,
                 repositoryDirectory: Path): CommandResult =
      %%('git, 'checkout, sha.value)(repositoryDirectory)
  }

  class Renku {

    private val triplesFileName = "triples.rdf"

    def logToFile(destinationDirectory: Path): TriplesFile = {
      val triplesStream = %%('renku, 'log, "--format", "rdf")(destinationDirectory).out.toInputStream
      val triplesFile = destinationDirectory / triplesFileName
      copy(triplesStream, triplesFile.toNIO)
      TriplesFile(triplesFile.toString())
    }

    implicit private class StreamValueOps(streamValue: StreamValue) {
      lazy val toInputStream: InputStream =
        new ByteArrayInputStream(streamValue.chunks.flatMap(_.array).toArray)
    }
  }
}