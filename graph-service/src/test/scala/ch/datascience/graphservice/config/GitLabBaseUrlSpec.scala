package ch.datascience.graphservice.config

import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class GitLabBaseUrlSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "return GitLab url if there's a valid value in the config for 'services.gitlab.url'" in {
      forAll(httpUrls) { url =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "gitlab" -> Map(
                "url" -> url
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = GitLabBaseUrl[Try](config)

        actual shouldBe GitLabBaseUrl(url)
      }
    }

    "fail if there's no relevant config entry" in {
      val Failure(exception) = GitLabBaseUrl[Try](ConfigFactory.empty())

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if config value is invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "url" -> "abcd"
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = GitLabBaseUrl[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
