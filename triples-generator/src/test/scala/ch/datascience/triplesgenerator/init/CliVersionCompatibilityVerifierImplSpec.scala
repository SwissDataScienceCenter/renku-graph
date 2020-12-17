package ch.datascience.triplesgenerator.init

import cats.data.NonEmptyList
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.triplesgenerator.generators.VersionGenerators._
import ch.datascience.generators.Generators.Implicits._

import scala.util.{Failure, Success, Try}

class CliVersionCompatibilityVerifierImplSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {
    "return Unit if the cli version matches the cli version from the first pair in compatibility matrix" in {
      val cliVersion = cliVersions.generateOne
      val versionPairs = renkuVersionPairs.generateNonEmptyList() match {
        case NonEmptyList(head, tail) => NonEmptyList(head.copy(cliVersion = cliVersion), tail)
      }
      val checker = new CliVersionCompatibilityVerifierImpl[Try](cliVersion, versionPairs)

      checker.run() shouldBe Success(())
    }

    "fail if the cli version does not match the cli version from the first pair in compatibility matrix" in {
      val cliVersion         = cliVersions.generateOne
      val versionPairs       = renkuVersionPairs.generateNonEmptyList()
      val checker            = new CliVersionCompatibilityVerifierImpl[Try](cliVersion, versionPairs)
      val Failure(exception) = checker.run()
      exception            shouldBe a[IllegalStateException]
      exception.getMessage shouldBe s"Incompatible versions. cliVersion: $cliVersion versionPairs: ${versionPairs.head.cliVersion}"
    }

  }

}
