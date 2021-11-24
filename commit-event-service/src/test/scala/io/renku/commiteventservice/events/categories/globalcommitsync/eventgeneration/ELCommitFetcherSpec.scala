package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.Encoder._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.{pages, pagingRequests}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.CommitId
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ELCommitFetcherSpec extends AnyWordSpec with IOSpec with ExternalServiceStubbing with should.Matchers {

  "fetchGitLabCommits" should {

    "fetch commits from the given page" in new TestCase {

      val maybeNextPage = pages.generateOption
      stubFor {
        get(s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}")
          .willReturn(
            okJson(commitIdsList.asJson.noSpaces)
              .withHeader("Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      elCommitFetcher
        .fetchELCommits(projectPath, pageRequest)
        .unsafeRunSync() shouldBe PageResult(commitIdsList, maybeNextPage)
    }

    "return no commits if there aren't any" in new TestCase {

      stubFor {
        get(s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}")
          .willReturn(okJson("[]"))
      }

      elCommitFetcher.fetchELCommits(projectPath, pageRequest).unsafeRunSync() shouldBe PageResult.empty
    }

    "return an empty list if project for NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}")
          .willReturn(notFound())
      }

      elCommitFetcher.fetchELCommits(projectPath, pageRequest).unsafeRunSync() shouldBe PageResult.empty
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}")
          .willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        elCommitFetcher.fetchELCommits(projectPath, pageRequest).unsafeRunSync()
      }.getMessage shouldBe s"GET $eventLogUrl/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage} returned ${Status.BadRequest}; body: some error"
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        elCommitFetcher.fetchELCommits(projectPath, pageRequest).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $eventLogUrl/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage} returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  private trait TestCase {
    val projectPath   = projectPaths.generateOne
    val pageRequest   = pagingRequests.generateOne
    val commitIdsList = commitIds.generateNonEmptyList().toList

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    private implicit val logger: TestLogger[IO] = TestLogger()
    val elCommitFetcher = new ELCommitFetcherImpl[IO](eventLogUrl)
  }

  private implicit lazy val encoder: Encoder[CommitId] = Encoder.instance { commitId =>
    json"""{
      "id": ${commitId.value},
      "status": ${eventStatuses.generateOne.value},
      "processingTimes": [],
      "date": ${timestampsNotInTheFuture.generateOne},
      "executionDate": ${timestampsNotInTheFuture.generateOne}
    }"""
  }
}
