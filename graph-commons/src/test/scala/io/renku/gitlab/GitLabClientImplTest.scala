package io.renku.gitlab

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import org.http4s.Method.GET
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
class GitLabClientImplTest extends AnyWordSpec with should.Matchers with MockFactory {

  "send" should {
    "succeed and call RestClient.send" +
      "and validate the URL" in new TestCase {
        val expectation = commitIds.generateOne

        client.send(GET,
                    urls.generateOne,
                    maybeAccessToken,
                    nonBlankStrings().generateOne,
                    queryParams.generateNonEmptyList().toList: _*
        )(_ => IO(expectation)) shouldBe IO(expectation)
      }

    "fail if the url is invalid" in new TestCase {
      val invalidURL = nonBlankStrings().generateOne

      client.send(GET,
                  invalidURL,
                  maybeAccessToken,
                  nonBlankStrings().generateOne,
                  queryParams.generateNonEmptyList().toList: _*
      )(_ => IO(commitIds.generateOne)) shouldBe IO(???)

    }

  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome

    val apiCallRecorder = mock[ApiCallRecorder[IO]]

    private implicit val logger: TestLogger[IO] = TestLogger()

    val client = new GitLabClientImpl[IO](apiCallRecorder, Throttler.noThrottling)

    lazy val urls: Gen[String] =
      Gen.const(s"${gitLabApiUrls.generateOne.value}/${nonBlankStrings().generateList().mkString("/")}")

    lazy val queryParams: Gen[(String, String)] = for {
      key   <- nonBlankStrings()
      value <- nonBlankStrings()
    } yield (key, value)
  }
}
