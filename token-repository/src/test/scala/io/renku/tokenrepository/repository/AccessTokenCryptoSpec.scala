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

package io.renku.tokenrepository.repository

import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.RefType
import io.renku.crypto.AesCrypto.Secret
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.client.AccessToken
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.nio.charset.StandardCharsets.UTF_8
import java.security.InvalidKeyException
import java.util.Base64
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class AccessTokenCryptoSpec extends AnyWordSpec with should.Matchers {

  "encrypt/decrypt" should {

    "encrypt and decrypt the given PersonalAccessToken" in new TestCase {
      val token: AccessToken = personalAccessTokens.generateOne

      val Success(crypted) = hookTokenCrypto.encrypt(token)
      crypted.value should not be token

      val Success(decrypted) = hookTokenCrypto.decrypt(crypted)
      decrypted shouldBe token
    }

    "encrypt and decrypt the given OauthAccessToken" in new TestCase {
      val token: AccessToken = oauthAccessTokens.generateOne

      val Success(crypted) = hookTokenCrypto.encrypt(token)
      crypted.value should not be token

      val Success(decrypted) = hookTokenCrypto.decrypt(crypted)
      decrypted shouldBe token
    }

    "fail if cannot be decrypted" in new TestCase {
      val token: EncryptedAccessToken = EncryptedAccessToken.from("abcd").fold(e => throw e, identity)

      val Failure(decryptException) = hookTokenCrypto.decrypt(token)

      decryptException            shouldBe an[Exception]
      decryptException.getMessage shouldBe "AccessToken decryption failed"
    }
  }

  "apply" should {

    "read the secret from the 'projects-tokens.secret' key in the config" in {
      val config = ConfigFactory.parseMap(
        Map(
          "projects-tokens" -> Map(
            "secret" -> aesCryptoSecrets.generateOne.value
          ).asJava
        ).asJava
      )

      val Success(crypto) = AccessTokenCrypto[Try](config)

      val token = accessTokens.generateOne
      crypto.encrypt(token) flatMap crypto.decrypt shouldBe Success(token)
    }

    "fail if there's wrong key in the 'projects-tokens.secret' key in the config" in {
      val invalidRawSecret = stringsOfLength(15).generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "projects-tokens" -> Map(
            "secret" -> new String(Base64.getEncoder.encode(invalidRawSecret.getBytes(UTF_8)), UTF_8)
          ).asJava
        ).asJava
      )

      val Failure(exception) = AccessTokenCrypto[Try](config)

      exception            shouldBe a[InvalidKeyException]
      exception.getMessage shouldBe "Invalid AES key length: 15 bytes"
    }
  }

  private trait TestCase {

    private val secret = new String(Base64.getEncoder.encode("1234567890123456".getBytes(UTF_8)), UTF_8)
    val hookTokenCrypto = new AccessTokenCryptoImpl[Try](
      RefType
        .applyRef[Secret](secret)
        .getOrElse(throw new IllegalArgumentException("Wrong secret"))
    )
  }
}
