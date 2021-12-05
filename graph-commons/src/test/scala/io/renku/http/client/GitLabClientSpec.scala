package io.renku.http.client

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.metrics.GitLabApiCallRecorder
import org.http4s.Method.GET
import org.http4s.Uri
import org.http4s.syntax.all._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GitLabClientSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "send" should {
    "succeed and call RestClient.send" +
      "and validate the URL" in new TestCase {
        val expectation = commitIds.generateOne

        client.send(GET,
                    uris.generateOne,
                    nonBlankStrings().generateOne,
        )(_ => IO(expectation)) shouldBe IO(expectation)
      }

  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome

    val apiCallRecorder = new GitLabApiCallRecorder( TestExecutionTimeRecorder[IO]())
    private implicit val logger: TestLogger[IO] = TestLogger()

    val client = new GitLabClientImpl[IO](gitLabApiUrls.generateOne, apiCallRecorder, Throttler.noThrottling)

    lazy val uris: Gen[Uri] =
      Gen.const(uri"" / gitLabApiUrls.generateOne.value / nonBlankStrings().generateList().mkString("/"))
  }
}
