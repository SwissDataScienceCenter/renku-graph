/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.creation

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Method.GET
import org.http4s.Status.{BadRequest, Forbidden, NotFound, Ok, Unauthorized}
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class TokenValidatorSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with GitLabClientTools[IO]
    with TableDrivenPropertyChecks
    with should.Matchers {

  "checkValid" should {

    "return true if call to the GL's Single Project API returns 200 with permissions property" in new TestCase {

      val projectId   = projectIds.generateOne
      val accessToken = accessTokens.generateOne

      val endpointName: String Refined NonEmpty = "single-project"
      val validationResult = results.generateOne
      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Boolean])(_: Option[AccessToken]))
        .expects(uri"projects" / projectId, endpointName, *, Option(accessToken))
        .returning(validationResult.pure[IO])

      validator.checkValid(projectId, accessToken).unsafeRunSync() shouldBe validationResult
    }

    forAll {
      Table(
        ("Case", "Response", "Expected Result"),
        ("ok valid", Response[IO](Ok).withEntity(json"""{"permissions": {}}"""), true),
        ("ok invalid", Response[IO](Ok).withEntity(json"""{}"""), false),
        ("unauthorized", Response[IO](Unauthorized), false),
        ("forbidden", Response[IO](Forbidden), false),
        ("notFound", Response[IO](NotFound), false)
      )
    } { (caze, response, result) =>
      show"map $caze to $result" in new TestCase {
        mapResponse(response.status, Request[IO](), response).unsafeRunSync() shouldBe result
      }
    }

    "throw an Exception if remote responds with status different than OK, NOT_FOUND or UNAUTHORIZED" in new TestCase {
      intercept[Exception] {
        mapResponse(BadRequest, Request[IO](), Response[IO](BadRequest)).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val validator = new TokenValidatorImpl[IO]

    lazy val mapResponse = captureMapping(gitLabClient)(
      findingMethod = validator.checkValid(projectIds.generateOne, accessTokens.generateOne).unsafeRunSync(),
      resultGenerator = results.generateOne,
      method = GET
    )
  }

  private lazy val results = Gen.oneOf(true, false)
}
