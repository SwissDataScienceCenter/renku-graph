package ch.datascience.triplesgenerator.config

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.auto._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class VersionCompatibilityConfigSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "apply" should {

    "fail if there are not value set for the matrix" in {

      val config = ConfigFactory.parseMap(
        Map("compatibility-matrix" -> List.empty[String].asJava).asJava
      )
      val Failure(exception) = VersionCompatibilityConfig[Try](config)
      exception            shouldBe a[Exception]
      exception.getMessage shouldBe "No compatibility matrix provided for schema version"
    }

    "return a list of VersionSchemaPairs if the value is set for the matrix" in {
      val cliVersionNumbers    = cliVersions.generateNonEmptyList(2, 10)
      val schemaVersionNumbers = projectSchemaVersions.generateNonEmptyList(2, 10)

      val expected =
        cliVersionNumbers.toList.zip(schemaVersionNumbers.toList).map { case (cliVersion, schemaVersion) =>
          VersionSchemaPair(cliVersion, schemaVersion)
        }

      val unparsedConfigElements = expected.map { case VersionSchemaPair(cliVersion, schemaVersion) =>
        s"${cliVersion.value} -> ${schemaVersion.value}"
      }.asJava

      val config = ConfigFactory.parseMap(
        Map("compatibility-matrix" -> unparsedConfigElements).asJava
      )

      val Success(result) = VersionCompatibilityConfig[Try](config)
      result shouldBe expected
    }

    "fail if pair is malformed" in {
      val malformedPair = "1.2.3 -> 12 -> 3"
      val config = ConfigFactory.parseMap(
        Map("compatibility-matrix" -> List(malformedPair).asJava).asJava
      )
      val Failure(exception) = VersionCompatibilityConfig[Try](config)
      exception            shouldBe a[Exception]
      exception.getMessage shouldBe s"Did not find exactly two elements: $malformedPair."
    }
  }
}
