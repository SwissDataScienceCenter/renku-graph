package io.renku.http.client

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock.{get, okJson, stubFor}
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.{pages, pagingRequests, personalAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.metrics.GitLabApiCallRecorder
import io.renku.testtools.IOSpec
import org.http4s.Method.GET
import org.http4s.Uri
import org.http4s.syntax.all._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GitLabClientSpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "send" should {
    "succeed and call RestClient.send and validate the URL" in new TestCase {
        val expectation = commitIds.generateOne

        client.send(GET,
                    uris.generateOne,
                    nonBlankStrings().generateOne,
        )(_ => IO(expectation)).unsafeRunSync() shouldBe expectation
      }


//    "" in new TestCase {
//      val projectId      = projectIds.generateOne
//      val commitInfoList = commitInfos.generateNonEmptyList().toList
//      val maybeNextPage = pages.generateOption
//
//
//      stubFor {
//        get(s"/api/v4/projects/$projectId/repository/commits?page=${pageRequest.page}&per_page=${pageRequest.perPage}")
//          .willReturn(
//            okJson(commitsJson(from = commitInfoList))
//              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
//          )
//      }
//    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome
    val pageRequest    = pagingRequests.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger()
    val apiCallRecorder = new GitLabApiCallRecorder( TestExecutionTimeRecorder[IO]())

    val client = new GitLabClientImpl[IO](gitLabApiUrls.generateOne, apiCallRecorder, Throttler.noThrottling)

     val uris: Gen[Uri] =
      Gen.const(uri"" / gitLabApiUrls.generateOne.value / nonBlankStrings().generateList().mkString("/"))
  }
}
