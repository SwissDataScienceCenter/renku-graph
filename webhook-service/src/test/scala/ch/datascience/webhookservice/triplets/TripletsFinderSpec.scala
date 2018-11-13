package ch.datascience.webhookservice.triplets

import ammonite.ops._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.triplets.Commands.{File, Git, Renku}
import ch.datascience.webhookservice.{CheckoutSha, GitRepositoryUrl}
import org.scalacheck.Gen
import org.scalamock.function.MockFunction0
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.ExecutionContext.Implicits.{global => executionContext}

class TripletsFinderSpec extends WordSpec with MockFactory with ScalaFutures with IntegrationPatience {

  "findTriplets" should {

    "clear the workspace" in new TestCase {

      inSequence {

        (file.mkdir(_: Path))
          .expects(repositoryDirectory)

        (git.cloneRepo(_: GitRepositoryUrl, _: Path, _: Path))
          .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
          .returning(CommandResult(0, Seq.empty))

        (git.checkout(_: CheckoutSha, _: Path))
          .expects(checkoutSha, repositoryDirectory)
          .returning(CommandResult(0, Seq.empty))

        (renku.log(_: Path))
          .expects(repositoryDirectory)
          .returning(tripletsAsString)

        (file.safeRemove(_: Path))
          .expects(repositoryDirectory)
      }

      tripletsFinder.findTriplets(gitRepositoryUrl, checkoutSha).futureValue shouldBe Right(tripletsAsString)
    }

    "return Left with exception if one is thrown when creating a repository directory" in new TestCase {
      val exception = new Exception("message")

      (file.mkdir(_: Path))
        .expects(repositoryDirectory)
        .throwing(exception)

      (file.safeRemove(_: Path))
        .expects(repositoryDirectory)

      tripletsFinder.findTriplets(gitRepositoryUrl, checkoutSha).futureValue shouldBe Left(exception)
    }

    "return Left with exception if one is thrown when cloning a repository" in new TestCase {
      val exception = new Exception("message")

      (file.mkdir(_: Path))
        .expects(repositoryDirectory)

      (git.cloneRepo(_: GitRepositoryUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .throwing(exception)

      (file.safeRemove(_: Path))
        .expects(repositoryDirectory)

      tripletsFinder.findTriplets(gitRepositoryUrl, checkoutSha).futureValue shouldBe Left(exception)
    }

    "return Left with exception if one is thrown when checking out the sha" in new TestCase {
      val exception = new Exception("message")

      (file.mkdir(_: Path))
        .expects(repositoryDirectory)

      (git.cloneRepo(_: GitRepositoryUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(CommandResult(0, Seq.empty))

      (git.checkout(_: CheckoutSha, _: Path))
        .expects(checkoutSha, repositoryDirectory)
        .throwing(exception)

      (file.safeRemove(_: Path))
        .expects(repositoryDirectory)

      tripletsFinder.findTriplets(gitRepositoryUrl, checkoutSha).futureValue shouldBe Left(exception)
    }

    "return Left with exception if one is thrown when calling 'renku log'" in new TestCase {
      val exception = new Exception("message")

      (file.mkdir(_: Path))
        .expects(repositoryDirectory)

      (git.cloneRepo(_: GitRepositoryUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(CommandResult(0, Seq.empty))

      (git.checkout(_: CheckoutSha, _: Path))
        .expects(checkoutSha, repositoryDirectory)
        .returning(CommandResult(0, Seq.empty))

      (renku.log(_: Path))
        .expects(repositoryDirectory)
        .throwing(exception)

      (file.safeRemove(_: Path))
        .expects(repositoryDirectory)

      tripletsFinder.findTriplets(gitRepositoryUrl, checkoutSha).futureValue shouldBe Left(exception)
    }
  }

  private trait TestCase {
    private val repositoryName = "some-repository"
    private val someRandomLong: Long = Gen.choose(1, 10).generateOne
    val workDirectory: Path = root / "tmp"
    val repositoryDirectory: Path = workDirectory / s"$repositoryName-$someRandomLong"
    val gitRepositoryUrl = GitRepositoryUrl(s"http://example.com/mike/$repositoryName.git")
    val checkoutSha: CheckoutSha = checkoutShas.generateOne
    val tripletsAsString: String = rawTriplets.generateOne

    val file: File = mock[Commands.File]
    val git: Git = mock[Commands.Git]
    val renku: Renku = mock[Commands.Renku]
    private val randomLong: MockFunction0[Long] = mockFunction[Long]
    randomLong.expects.returning(someRandomLong)
    val tripletsFinder = new TripletsFinder(file, git, renku, randomLong)
  }
}
