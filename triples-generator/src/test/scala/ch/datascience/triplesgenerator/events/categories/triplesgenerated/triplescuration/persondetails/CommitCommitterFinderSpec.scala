package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonDetailsGenerators._
import com.github.tomakehurst.wiremock.client.WireMock.{get, okJson, stubFor}
import io.circe.Json
import io.circe.literal.JsonStringContext
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class CommitCommitterFinderSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {
  "findCommitPeople" should {
    "return a CommitPersonInfo 2 CommitPersons when both the author and committer were found" in new TestCase {
      val expectedCommitPersonInfo               = commitPersonInfos.generateOne
      val NonEmptyList(author, committer +: Nil) = expectedCommitPersonInfo.committers

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectPath.toString)}/repository/commits/${urlEncode(commitId.toString)}")
          .willReturn(okJson(gitLabResponse(commitId, author, committer).toString()))
      }

      commitCommitterFinder.findCommitPeople(projectPath, commitId).unsafeRunSync() shouldBe expectedCommitPersonInfo
    }
  }

  private implicit lazy val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit lazy val timer: Timer[IO]        = IO.timer(global)

  trait TestCase {
    private val gitLabUrl = GitLabUrl(externalServiceBaseUrl)

    val commitCommitterFinder =
      new CommitCommitterFinderImpl[IO](gitLabUrl.apiV4, Throttler.noThrottling, TestLogger(), retryInterval = 1 millis)

    val projectPath = projectPaths.generateOne
    val commitId    = commitIds.generateOne

    def gitLabResponse(commitId: CommitId, author: CommitPerson, committer: CommitPerson): Json =
      json"""
            {
              "id": "${commitId.toString}",
              "short_id": "6104942438c",
              "title": "Sanitize for network graph",
              "author_name": ${author.name.value},
              "author_email": ${author.email.value},
              "committer_name": ${committer.name.value},
              "committer_email": ${committer.email.value},
              "created_at": "2012-09-20T09:06:12+03:00",
              "message": "Sanitize for network graph",
              "committed_date": "2012-09-20T09:06:12+03:00",
              "authored_date": "2012-09-20T09:06:12+03:00",
              "parent_ids": [
                "ae1d9fb46aa2b07ee9836d49862ec4e2c46fbbba"
              ],
              "last_pipeline" : {
                "id": 8,
                "ref": "master",
                "sha": "2dc6aa325a317eda67812f05600bdf0fcdc70ab0",
                "status": "created"
              },
              "stats": {
                "additions": 15,
                "deletions": 10,
                "total": 25
              },
              "status": "running",
              "web_url": "https://gitlab.example.com/thedude/gitlab-foss/-/commit/6104942438c14ec7bd21c6cd5bd995272b3faff6"
            }
            """
  }
}
