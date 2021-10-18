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

package io.renku.webhookservice.crypto

import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.RefType
import eu.timepit.refined.auto._
import io.renku.crypto.AesCrypto.Secret
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.model.HookToken
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.nio.charset.StandardCharsets.UTF_8
import java.security.InvalidKeyException
import java.util.Base64
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class HookTokenCryptoSpec extends AnyWordSpec with should.Matchers {

  "encrypt/decrypt" should {

    "encrypt and decrypt a given HookToken" in new TestCase {
      val token: HookToken = hookTokens.generateOne

      val Success(crypted) = hookTokenCrypto.encrypt(token)
      crypted.value should not be token

      val Success(decrypted) = hookTokenCrypto.decrypt(crypted)
      decrypted shouldBe token
    }

    "fail if cannot be decrypted" in new TestCase {
      val token: SerializedHookToken = SerializedHookToken.from("abcd").fold(e => throw e, identity)

      val Failure(decryptException) = hookTokenCrypto.decrypt(token)

      decryptException            shouldBe an[Exception]
      decryptException.getMessage shouldBe "HookToken decryption failed"
    }
  }

  "apply" should {

    "read the secret from the 'services.gitlab.hook-token-secret' key in the config" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "hook-token-secret" -> aesCryptoSecrets.generateOne.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Success(crypto) = HookTokenCrypto[Try](config)

      val token = hookTokens.generateOne
      crypto.encrypt(token) flatMap crypto.decrypt shouldBe Success(token)
    }

    "fail if there's wrong key in the 'services.gitlab.hook-token-secret' key in the config" in {
      val invalidRawSecret = stringsOfLength(15).generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "hook-token-secret" -> new String(Base64.getEncoder.encode(invalidRawSecret.getBytes(UTF_8)), UTF_8)
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = HookTokenCrypto[Try](config)

      exception            shouldBe a[InvalidKeyException]
      exception.getMessage shouldBe "Invalid AES key length: 15 bytes"
    }
  }

  private trait TestCase {

    private val secret = new String(Base64.getEncoder.encode("1234567890123456".getBytes("utf-8")), "utf-8")
    val hookTokenCrypto = new HookTokenCrypto[Try](
      RefType
        .applyRef[Secret](secret)
        .getOrElse(throw new IllegalArgumentException("Wrong secret"))
    )
  }
}
