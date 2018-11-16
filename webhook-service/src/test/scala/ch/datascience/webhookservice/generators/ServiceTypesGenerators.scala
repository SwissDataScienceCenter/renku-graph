package ch.datascience.webhookservice.generators

import ch.datascience.generators.Generators._
import ch.datascience.webhookservice._
import ch.datascience.webhookservice.queue.TriplesFile
import org.scalacheck.Gen

object ServiceTypesGenerators {

  val shas: Gen[String] = for {
    length <- Gen.choose(5, 40)
    chars <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'f').map(_.toString)))
  } yield chars.mkString("")

  implicit val filePaths: Gen[FilePath] = relativePaths map FilePath

  implicit val checkoutShas: Gen[CheckoutSha] = shas map CheckoutSha

  implicit val gitRepositoryUrls: Gen[GitRepositoryUrl] =
    nonEmptyStrings()
      .map { repoName =>
        GitRepositoryUrl(s"http://host/$repoName.git")
      }

  implicit val projectNames: Gen[ProjectName] = nonEmptyStrings() map ProjectName

  implicit val pushEvents: Gen[PushEvent] = for {
    sha <- checkoutShas
    repositoryUrl <- gitRepositoryUrls
    projectName <- projectNames
  } yield PushEvent(sha, repositoryUrl, projectName)

  implicit val triplesFiles: Gen[TriplesFile] = relativePaths map TriplesFile
}
