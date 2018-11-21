package ch.datascience.webhookservice

import ch.datascience.tinytypes.StringValue
import ch.datascience.tinytypes.constraints.NonBlank
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

class FilePathSpec extends WordSpec with PropertyChecks {

  import ch.datascience.generators.Generators._

  "FilePath" should {

    "be a NonBlank StringValue" in {
      val filePath = FilePath( "abc" )

      filePath shouldBe a[StringValue]
      filePath shouldBe a[NonBlank]
    }

    "be instantiable when value is a relative path" in {
      forAll( relativePaths ) { path =>
        FilePath( path ).toString shouldBe path
      }
    }

    "throw na exception when value starts with a '/'" in {
      intercept[IllegalArgumentException] {
        FilePath( "/abc" )
      }.getMessage shouldBe "'/abc' is not a valid FilePath"
    }
  }
}

class GitShaSpec extends WordSpec with PropertyChecks {

  import ch.datascience.generators.Generators.Implicits._
  import ch.datascience.webhookservice.generators.ServiceTypesGenerators._

  private case class SomeGitSha( value: String ) extends GitSha

  "GitSha" should {

    "be a NonBlank StringValue" in {
      SomeGitSha( shas.generateOne ) shouldBe a[StringValue]
      SomeGitSha( shas.generateOne ) shouldBe a[NonBlank]
    }

    "be instantiatable for a valid sha" in {
      forAll( shas ) { sha =>
        SomeGitSha( sha ).toString shouldBe sha
      }
    }

    "throw an IllegalArgumentException for non-sha values" in {
      intercept[IllegalArgumentException] {
        SomeGitSha( "abc" )
      }.getMessage shouldBe "'abc' is not a valid Git sha"
    }
  }
}
