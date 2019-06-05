package ch.datascience.graphservice.rdfstore

import RDFStoreGenerators._
import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators.Implicits._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class RDFStoreConfigSpec extends WordSpec with ScalaCheckPropertyChecks {

  "get" should {

    "read 'services.fuseki.url', 'services.fuseki.dataset-name' to instantiate the RDFStoreConfig" in {
      forAll(fusekiConfigs) { fusekiConfig =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "fuseki" -> Map(
                "url"          -> fusekiConfig.fusekiBaseUrl.toString,
                "dataset-name" -> fusekiConfig.datasetName.value
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = RDFStoreConfig[Try](config)

        actual.fusekiBaseUrl shouldBe fusekiConfig.fusekiBaseUrl
        actual.datasetName   shouldBe fusekiConfig.datasetName
      }
    }

    "fail if url invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> "invalid-url",
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RDFStoreConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if dataset-name is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> "  "
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RDFStoreConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
