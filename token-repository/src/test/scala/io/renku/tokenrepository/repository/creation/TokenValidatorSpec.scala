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

package io.renku.tokenrepository.repository.creation

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Method.HEAD
import org.http4s.Status.{BadRequest, Forbidden, NotFound, Ok, Unauthorized}
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

    "return true if HEAD /user call to the the GL returns 200" in new TestCase {

      val accessToken = accessTokens.generateOne

      val endpointName: String Refined NonEmpty = "user"
      val validationResult = results.generateOne
      (gitLabClient
        .head(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Boolean])(_: Option[AccessToken]))
        .expects(uri"user", endpointName, *, Option(accessToken))
        .returning(validationResult.pure[IO])

      validator.checkValid(accessToken).unsafeRunSync() shouldBe validationResult
    }

    forAll {
      Table(
        "Status"     -> "Expected Result",
        Ok           -> true,
        Unauthorized -> false,
        Forbidden    -> false,
        NotFound     -> false
      )
    } { (status, result) =>
      s"map $status to true" in new TestCase {
        mapResponse(status, Request[IO](), Response[IO](status)).unsafeRunSync() shouldBe result
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

    lazy val mapResponse = captureMapping(validator, gitLabClient)(
      findingMethod = _.checkValid(accessTokens.generateOne).unsafeRunSync(),
      resultGenerator = results.generateOne,
      method = HEAD
    )
  }

  private lazy val results = Gen.oneOf(true, false)
}
