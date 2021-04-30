/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.oauthAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonDetailsGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal.JsonStringContext
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class CommitCommitterFinderSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "findCommitPeople" should {

    "return a CommitPersonInfo 2 CommitPersons when both the author and committer were found" in new TestCase {
      val accessToken = oauthAccessTokens.generateOne
      val author      = commitPersons.generateOne
      val committer   = commitPersons.generateOne
      val expectedCommitPersonInfo =
        commitPersonInfos.generateOne.copy(committers = NonEmptyList(author, committer +: Nil))

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectId.toString)}/repository/commits/${expectedCommitPersonInfo.id}")
          .willReturn(okJson(gitLabResponse(expectedCommitPersonInfo.id, author, committer).toString()))
      }

      commitCommitterFinder
        .findCommitPeople(projectId, expectedCommitPersonInfo.id, accessToken.some)
        .value
        .unsafeRunSync() shouldBe Right(expectedCommitPersonInfo)
    }

    "return an CurationRecoverableError if remote client responds with UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectId.toString)}/repository/commits/$commitId")
          .willReturn(unauthorized())
      }

      commitCommitterFinder
        .findCommitPeople(projectId, commitId, maybeAccessToken = None)
        .value
        .unsafeRunSync() shouldBe Either.left[ProcessingRecoverableError, CommitPersonsInfo](
        CurationRecoverableError("Access token not valid to fetch project commit info")
      )
    }

    "return an CurationRecoverableError if remote client responds with SERVICE_UNAVAILABLE" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectId.toString)}/repository/commits/$commitId")
          .willReturn(serviceUnavailable())
      }

      commitCommitterFinder
        .findCommitPeople(projectId, commitId, maybeAccessToken = None)
        .value
        .unsafeRunSync() shouldBe Either.left[ProcessingRecoverableError, CommitPersonsInfo](
        CurationRecoverableError("Service unavailable")
      )
    }

    "return an CurationRecoverableError if there's a connectivity problem" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectId.toString)}/repository/commits/$commitId")
          .willReturn(aResponse().withFault(CONNECTION_RESET_BY_PEER))
      }

      val Left(error) = commitCommitterFinder
        .findCommitPeople(projectId, commitId, maybeAccessToken = None)
        .value
        .unsafeRunSync()

      error shouldBe a[CurationRecoverableError]
    }

    "return an CurationRecoverableError if there's other client error" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectId.toString)}/repository/commits/$commitId")
          .willReturn(aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt))
      }

      val Left(error) = commitCommitterFinder
        .findCommitPeople(projectId, commitId, maybeAccessToken = None)
        .value
        .unsafeRunSync()

      error shouldBe a[CurationRecoverableError]
    }

    "return an Error if remote client responds with invalid json" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectId.toString)}/repository/commits/$commitId")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        commitCommitterFinder.findCommitPeople(projectId, commitId, maybeAccessToken = None).value.unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }

    "return an Error if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectId.toString)}/repository/commits/$commitId")
          .willReturn(notFound().withBody("some message"))
      }

      intercept[Exception] {
        commitCommitterFinder.findCommitPeople(projectId, commitId, maybeAccessToken = None).value.unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId returned ${Status.NotFound}; body: some message"
    }
  }

  private implicit lazy val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit lazy val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val gitLabUrl      = GitLabUrl(externalServiceBaseUrl)
    val requestTimeout = 2 seconds

    val commitCommitterFinder = new CommitCommitterFinderImpl(gitLabUrl.apiV4,
                                                              Throttler.noThrottling,
                                                              TestLogger(),
                                                              retryInterval = 100 millis,
                                                              maxRetries = 1,
                                                              requestTimeoutOverride = Some(requestTimeout)
    )

    val projectId = projectIds.generateOne
    val commitId  = commitIds.generateOne

    def gitLabResponse(commitId: CommitId, author: CommitPerson, committer: CommitPerson): Json =
      json"""{
              "id": ${commitId.toString},
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
