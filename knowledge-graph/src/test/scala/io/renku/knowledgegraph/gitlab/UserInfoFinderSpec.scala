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

package io.renku.knowledgegraph.gitlab

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.core.client.Generators.userInfos
import io.renku.core.client.UserInfo
import io.renku.generators.CommonGraphGenerators.userAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.testtools.GitLabClientTools
import org.http4s.Status.{NotFound, Ok}
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class UserInfoFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with EitherValues
    with GitLabClientTools[IO] {

  it should "call GL's GET gl/user and return the user info" in {

    val accessToken  = userAccessTokens.generateOne
    val someUserInfo = userInfos.generateSome

    givenFetchUserInfo(
      accessToken,
      returning = someUserInfo.pure[IO]
    )

    finder.findUserInfo(accessToken).asserting(_ shouldBe someUserInfo)
  }

  it should "return some user info if GL returns 200 with relevant values in the payload" in {
    val userInfo = userInfos.generateOne
    mapResponse(Ok, Request[IO](), Response[IO](Ok).withEntity(userInfo.asJson))
      .asserting(_ shouldBe Some(userInfo))
  }

  it should "return no user info if GL returns 404 NOT_FOUND" in {
    mapResponse(NotFound, Request[IO](), Response[IO](NotFound))
      .asserting(_ shouldBe None)
  }

  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val finder = new UserInfoFinderImpl[IO]

  private def givenFetchUserInfo(accessToken: AccessToken, returning: IO[Option[UserInfo]]) = {
    val endpointName: String Refined NonEmpty = "user"
    (glClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[UserInfo]])(_: Option[AccessToken]))
      .expects(uri"user", endpointName, *, accessToken.some)
      .returning(returning)
  }

  private lazy val mapResponse: ResponseMappingF[IO, Option[UserInfo]] =
    captureMapping(glClient)(
      finder
        .findUserInfo(userAccessTokens.generateOne)
        .unsafeRunSync(),
      userInfos.toGeneratorOfOptions,
      underlyingMethod = Get
    )

  private implicit lazy val payloadEncoder: Encoder[UserInfo] = Encoder.instance { case UserInfo(name, email) =>
    json"""{
      "name":  $name,
      "email": $email
    }"""
  }
}
