/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository

import java.util.Base64

import ch.datascience.http.client.AccessToken
import ch.datascience.crypto.AesCrypto.Secret
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.GraphCommonsGenerators._
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import eu.timepit.refined.api.RefType
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class AccessTokenCryptoSpec extends WordSpec {

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

  private trait TestCase {

    import cats.implicits._

    private val secret = new String(Base64.getEncoder.encode("1234567890123456".getBytes("utf-8")), "utf-8")
    val hookTokenCrypto = new AccessTokenCrypto[Try](
      RefType
        .applyRef[Secret](secret)
        .getOrElse(throw new IllegalArgumentException("Wrong secret"))
    )
  }
}
