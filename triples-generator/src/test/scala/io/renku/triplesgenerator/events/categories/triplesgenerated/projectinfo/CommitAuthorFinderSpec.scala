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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import eu.timepit.refined.auto._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonBlankStrings
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects.Path
import io.renku.graph.model.{GitLabUrl, users}
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.http4s.Status.{Forbidden, ServiceUnavailable, Unauthorized}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.language.{postfixOps, reflectiveCalls}

class CommitAuthorFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks
    with TinyTypeEncoders {

  "findCommitAuthor" should {

    "return commit author's name and email if found" in new TestCase {
      forAll { (authorName: users.Name, authorEmail: users.Email) =>
        `/api/v4/projects/:id/repository/commits/:sha`(projectPath, commitId) returning okJson(
          (authorName -> authorEmail).asJson.noSpaces
        )

        finder
          .findCommitAuthor(projectPath, commitId)
          .value
          .unsafeRunSync() shouldBe (authorName -> authorEmail).some.asRight
      }
    }

    "return None if commit NOT_FOUND" in new TestCase {
      `/api/v4/projects/:id/repository/commits/:sha`(projectPath, commitId) returning notFound()

      finder
        .findCommitAuthor(projectPath, commitId)
        .value
        .unsafeRunSync() shouldBe None.asRight
    }

    "return None if commit author not found" in new TestCase {
      `/api/v4/projects/:id/repository/commits/:sha`(projectPath, commitId) returning okJson("{}")

      finder
        .findCommitAuthor(projectPath, commitId)
        .value
        .unsafeRunSync() shouldBe None.asRight
    }

    "return None if commit author email invalid" in new TestCase {
      `/api/v4/projects/:id/repository/commits/:sha`(projectPath, commitId) returning okJson {
        json"""{
          "author_name":  ${userNames.generateOne},
          "author_email": ${nonBlankStrings().generateOne.value}
        }""".noSpaces
      }

      finder
        .findCommitAuthor(projectPath, commitId)
        .value
        .unsafeRunSync() shouldBe None.asRight
    }

    Set(
      "connection problem" -> aResponse().withFault(CONNECTION_RESET_BY_PEER),
      "client problem"     -> aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt),
      "ServiceUnavailable" -> aResponse().withStatus(ServiceUnavailable.code),
      "Forbidden"          -> aResponse().withStatus(Forbidden.code),
      "Unauthorized"       -> aResponse().withStatus(Unauthorized.code)
    ) foreach { case (problemName, response) =>
      s"return a Recoverable Failure for $problemName when fetching commit author" in new TestCase {
        `/api/v4/projects/:id/repository/commits/:sha`(projectPath, commitId) returning response

        val Left(failure) = finder.findCommitAuthor(projectPath, commitId).value.unsafeRunSync()
        failure shouldBe a[ProcessingRecoverableError]
      }
    }
  }

  private lazy val requestTimeout = 2 seconds

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val projectPath = projectPaths.generateOne
    val commitId    = commitIds.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabUrl = GitLabUrl(externalServiceBaseUrl).apiV4
    val finder = new CommitAuthorFinderImpl[IO](gitLabUrl,
                                                Throttler.noThrottling,
                                                retryInterval = 100 millis,
                                                maxRetries = 1,
                                                requestTimeoutOverride = Some(requestTimeout)
    )
  }

  private implicit lazy val authorEncoder: Encoder[(users.Name, users.Email)] = Encoder.instance {
    case (authorName, authorEmail) => json"""{
      "author_name":  $authorName,
      "author_email": $authorEmail
    }"""
  }

  private def `/api/v4/projects/:id/repository/commits/:sha`(path: Path, commitId: CommitId)(implicit
      maybeAccessToken:                                            Option[AccessToken]
  ) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.value)}/repository/commits/$commitId")
        .withAccessToken(maybeAccessToken)
        .willReturn(response)
    }
  }
}
