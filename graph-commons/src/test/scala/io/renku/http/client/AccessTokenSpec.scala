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

package io.renku.http.client

import io.circe.DecodingFailure
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.renku.tinytypes.Sensitive
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

class AccessTokenSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {
  val base64Encoder = Base64.getEncoder

  "PersonalAccessToken" should {

    "be Sensitive" in {
      personalAccessTokens.generateOne shouldBe a[Sensitive]
    }

    "be instantiatable from a non-blank String" in {
      forAll(nonEmptyStrings()) { value =>
        PersonalAccessToken.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail instantiation for a blank String" in {
      val Left(exception) = PersonalAccessToken.from(" ")

      exception shouldBe an[IllegalArgumentException]
    }
  }

  "OAuthAccessToken" should {

    "be Sensitive" in {
      oauthAccessTokens.generateOne shouldBe a[Sensitive]
    }

    "be instantiatable from a non-blank String" in {
      forAll(nonEmptyStrings()) { value =>
        OAuthAccessToken.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail instantiation for a blank String" in {
      val Left(exception) = OAuthAccessToken.from(" ")

      exception shouldBe an[IllegalArgumentException]
    }
  }

  "accessToken json decoder" should {

    "decode OAuthAccessToken" in {
      val accessToken  = oauthAccessTokens.generateOne
      val encodedToken = new String(base64Encoder.encode(accessToken.value.getBytes(UTF_8)), UTF_8)
      json"""{"oauthAccessToken": $encodedToken}""".as[AccessToken] shouldBe Right(accessToken)
    }

    "decode PersonalAccessToken" in {
      val accessToken  = personalAccessTokens.generateOne
      val encodedToken = new String(base64Encoder.encode(accessToken.value.getBytes(UTF_8)), UTF_8)
      json"""{"personalAccessToken": $encodedToken}""".as[AccessToken] shouldBe Right(accessToken)
    }

    "fail for a invalid access token json" in {
      val Left(failure) = json"""{"someToken": "value"}""".as[AccessToken]
      failure shouldBe DecodingFailure("Access token cannot be deserialized", Nil)
    }
  }

  "accessToken json encoder" should {

    "encode OAuthAccessToken" in {
      val accessToken: AccessToken = oauthAccessTokens.generateOne
      val encodedToken = new String(base64Encoder.encode(accessToken.value.getBytes(UTF_8)), UTF_8)
      accessToken.asJson shouldBe json"""{"oauthAccessToken": $encodedToken}"""
    }

    "encode PersonalAccessToken" in {
      val accessToken: AccessToken = personalAccessTokens.generateOne
      val encodedToken = new String(base64Encoder.encode(accessToken.value.getBytes(UTF_8)), UTF_8)
      accessToken.asJson shouldBe json"""{"personalAccessToken": $encodedToken}"""
    }
  }
}
