/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.association

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.{accessTokens, projectAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.durations
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Method.POST
import org.http4s.Status.{Created, Unauthorized}
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{LocalDate, Period}
import scala.concurrent.duration._

class ProjectAccessTokenCreatorSpec
    extends AnyWordSpec
    with MockFactory
    with GitLabClientTools[IO]
    with IOSpec
    with should.Matchers {

  "createPersonalAccessToken" should {

    "do POST projects/:id/access_tokens with relevant payload" in new TestCase {

      val endpointName: String Refined NonEmpty = "create-project-access-token"
      val payload = json"""{
        "name":       "renku",
        "scopes":     ["api", "read_repository"],
        "expires_at": ${now plus projectTokenTTL}
      }"""
      val projectAccessToken = projectAccessTokens.generateOne
      (gitLabClient
        .post(_: Uri, _: String Refined NonEmpty, _: Json)(_: ResponseMappingF[IO, ProjectAccessToken])(
          _: Option[AccessToken]
        ))
        .expects(uri"projects" / projectId.value / "access_tokens", endpointName, payload, *, Option(accessToken))
        .returning(projectAccessToken.pure[IO])

      tokenCreator.createPersonalAccessToken(projectId, accessToken).unsafeRunSync() shouldBe projectAccessToken
    }

    s"retrieve the created Project Access Token from the response with $Created status" in new TestCase {
      val createdToken = projectAccessTokens.generateOne
      mapResponse(Created, Request[IO](), Response[IO](Created).withEntity(glResponsePayload(createdToken)))
        .unsafeRunSync() shouldBe createdToken
    }

    s"fail for responses other than $Created" in new TestCase {
      intercept[Exception] {
        mapResponse(Unauthorized, Request[IO](), Response[IO](Unauthorized)).unsafeRunSync()
      }
    }
  }

  private trait TestCase {

    val now         = LocalDate.now()
    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    val currentDate = mockFunction[LocalDate]
    currentDate.expects().returning(now)
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val projectTokenTTL = Period.ofDays(durations(1 day, 730 days).generateOne.toDays.toInt)
    val tokenCreator    = new ProjectAccessTokenCreatorImpl[IO](projectTokenTTL, currentDate)

    lazy val mapResponse = captureMapping(tokenCreator, gitLabClient)(
      findingMethod = _.createPersonalAccessToken(projectId, accessToken).unsafeRunSync(),
      resultGenerator = projectAccessTokens.generateOne,
      method = POST
    )
  }

  private def glResponsePayload(projectAccessToken: ProjectAccessToken) = json"""{
    "token": ${projectAccessToken.value}
  }"""
}
