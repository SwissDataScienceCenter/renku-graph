package ch.datascience.webhookservice.queue

import java.io.{ByteArrayInputStream, InputStream}
import java.security.SecureRandom

import ch.datascience.webhookservice.queue.Commands.{File, Git, Renku}
import ch.datascience.webhookservice.{CheckoutSha, GitRepositoryUrl}
import org.w3.banana.io.{NTriples, NTriplesReader, RDFReader}
import org.w3.banana.jena.Jena

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Try

class TripletsFinder(file: Commands.File,
                     git: Commands.Git,
                     renku: Commands.Renku,
                     randomLong: () => Long,
                     tripletsReader: RDFReader[Jena, Try, NTriples]) {

  import TripletsFinder._
  import file._

  def findRdfGraph(gitRepositoryUrl: GitRepositoryUrl,
                   checkoutSha: CheckoutSha)
                  (implicit executionContext: ExecutionContext): Future[Either[Throwable, Jena#Graph]] = Future {

    val repositoryDirectory = tempDirectoryName(repositoryDirName(gitRepositoryUrl))

    val maybeRdfGraph = for {
      _ <- pure(mkdir(repositoryDirectory))
      _ <- pure(git cloneRepo(gitRepositoryUrl, repositoryDirectory, workDirectory))
      _ <- pure(git checkout(checkoutSha, repositoryDirectory))
      tripletsStream <- pure(renku log repositoryDirectory)
      rdfGraph <- tripletsReader.read(tripletsStream, "")
      _ <- pure(removeSilently(repositoryDirectory))
    } yield rdfGraph

    maybeRdfGraph.fold(
      exception => {
        removeSilently(repositoryDirectory)
        Left(exception)
      },
      tripletAsString => Right(tripletAsString)
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

object TripletsFinder {

  import ammonite.ops.{Path, root}

  private val workDirectory: Path = root / "tmp"
  private val repositoryDirectoryFinder = ".*/(.*).git$".r

  def apply(): TripletsFinder = new TripletsFinder(
    file = new File(),
    git = new Git(),
    renku = new Renku(),
    randomLong = new SecureRandom().nextLong,
    tripletsReader = new NTriplesReader
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

    def log(destinationDirectory: Path): InputStream =
      %%('renku, 'log, "--format", "nt")(destinationDirectory).out.toInputStream

    implicit private class StreamValueOps(streamValue: StreamValue) {
      lazy val toInputStream: InputStream =
        new ByteArrayInputStream(streamValue.chunks.flatMap(_.array).toArray)
    }
  }
}