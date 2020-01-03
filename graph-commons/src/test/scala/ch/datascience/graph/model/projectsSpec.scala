package ch.datascience.graph.model

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.tinytypes.constraints.RelativePath
import eu.timepit.refined.auto._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectPathSpec extends WordSpec with ScalaCheckPropertyChecks {

  import org.scalacheck.Gen.{alphaChar, const, frequency, numChar, oneOf}

  "ProjectPath" should {

    "be a RelativePath" in {
      ProjectPath shouldBe a[RelativePath]
    }
  }

  "instantiation" should {

    "be successful for relative paths with min number of 2 segments" in {
      forAll(relativePaths(minSegments = 2, maxSegments = 22, partsGenerator)) { path =>
        ProjectPath(path).value shouldBe path
      }
    }

    "fail for relative paths of single segment" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(nonBlankStrings().generateOne.value)
      }
    }

    "fail when ending with a /" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(relativePaths(minSegments = 2, maxSegments = 22).generateOne + "/")
      }
    }

    "fail for absolute URLs" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(httpUrls.generateOne)
      }
    }
  }

  private val partsGenerator = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> const('_'))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf('_', '.', '-'))
    for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"
  }
}

class FullProjectPathSpec extends WordSpec with ScalaCheckPropertyChecks {

  import org.scalacheck.Gen.{alphaChar, const, frequency, numChar, oneOf}

  "ProjectPath" should {

    "be a RelativePath" in {
      ProjectPath shouldBe a[RelativePath]
    }
  }

  "instantiation" should {

    "be successful for relative paths with min number of 2 segments" in {
      forAll(relativePaths(minSegments = 2, maxSegments = 22, partsGenerator)) { path =>
        ProjectPath(path).value shouldBe path
      }
    }

    "fail for relative paths of single segment" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(nonBlankStrings().generateOne.value)
      }
    }

    "fail when ending with a /" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(relativePaths(minSegments = 2, maxSegments = 22).generateOne + "/")
      }
    }

    "fail for absolute URLs" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(httpUrls.generateOne)
      }
    }
  }

  private val partsGenerator = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> const('_'))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf('_', '.', '-'))
    for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"
  }
}
