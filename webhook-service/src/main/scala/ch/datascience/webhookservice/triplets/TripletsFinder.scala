package ch.datascience.webhookservice.triplets

import java.security.SecureRandom

import ch.datascience.webhookservice.triplets.Commands.{File, Git, Renku}
import ch.datascience.webhookservice.{CheckoutSha, GitRepositoryUrl}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Try

class TripletsFinder private[triplets](file: Commands.File,
                                       git: Commands.Git,
                                       renku: Commands.Renku,
                                       randomLong: () => Long) {
  import TripletsFinder._
  import file._

  def findTriplets(gitRepositoryUrl: GitRepositoryUrl,
                   checkoutSha: CheckoutSha)
                  (implicit executionContext: ExecutionContext): Future[Either[Throwable, String]] = Future {

    val repositoryDirectory = tempDirectoryName(repositoryDirName(gitRepositoryUrl))

    val maybeTriplets = for {
      _ <- Try(mkdir(repositoryDirectory))
      _ <- pure(git cloneRepo(gitRepositoryUrl, repositoryDirectory, workDirectory))
      _ <- pure(git checkout(checkoutSha, repositoryDirectory))
      tripletsAsString <- pure(renku log repositoryDirectory)
      _ <- pure(safeRemove(repositoryDirectory))
    } yield tripletsAsString

    maybeTriplets.fold(
      exception => {
        safeRemove(repositoryDirectory)
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
    randomLong = new SecureRandom().nextLong
  )
}

private object Commands {

  import ammonite.ops
  import ammonite.ops._

  class File {


    def mkdir(newDir: Path): Unit = ops.mkdir ! newDir

    def safeRemove(repositoryDirectory: Path): Unit = Try {
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

    def log(destinationDirectory: Path): String =
      %%('renku, 'log)(destinationDirectory).out.string
  }
}