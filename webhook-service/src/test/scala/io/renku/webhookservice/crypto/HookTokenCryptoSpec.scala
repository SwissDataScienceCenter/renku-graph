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

package io.renku.webhookservice.crypto

import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.crypto.AesCrypto.Secret
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.model.HookToken
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import scala.jdk.CollectionConverters._
import scala.util.Try

class HookTokenCryptoSpec extends AnyWordSpec with should.Matchers with TryValues {

  "encrypt/decrypt" should {

    "encrypt and decrypt a given HookToken" in new TestCase {
      val token: HookToken = hookTokens.generateOne

      val crypted = hookTokenCrypto.encrypt(token).success.value
      crypted.value should not be token

      val decrypted = hookTokenCrypto.decrypt(crypted).success.value
      decrypted shouldBe token
    }

    "fail if cannot be decrypted" in new TestCase {
      val token: SerializedHookToken = SerializedHookToken.from("abcd").fold(e => throw e, identity)

      val decryptException = hookTokenCrypto.decrypt(token).failure.exception

      decryptException            shouldBe an[Exception]
      decryptException.getMessage shouldBe "HookToken decryption failed"
    }

    "decrypt existing values" in {
      val usedSecret      = Secret.unsafeFromBase64("YWJjZGVmZzEyMzQ1Njc4OQ==")
      val hookTokenCrypto = new HookTokenCryptoImpl[Try](usedSecret)

      val plain =
        hookTokenCrypto.decrypt(
          SerializedHookToken.from("4TV8A81+1+U3dfa8sVO9TUuMvGHLaerySYpxEIkVT/U=").fold(throw _, identity)
        )

      plain.success.value shouldBe HookToken(123456)
    }
  }

  "apply" should {

    "read the secret from the 'services.gitlab.hook-token-secret' key in the config" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "hook-token-secret" -> aesCryptoSecrets.generateOne.decodeAscii.fold(throw _, identity)
            ).asJava
          ).asJava
        ).asJava
      )

      val crypto = HookTokenCrypto[Try](config).success.value

      val token = hookTokens.generateOne
      (crypto.encrypt(token) flatMap crypto.decrypt).success.value shouldBe token
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

      val exception = HookTokenCrypto[Try](config).failure.exception

      exception          shouldBe a[ConfigLoadingException]
      exception.getMessage should include("Expected 16 or 24 or 32 bytes, but got 15")
    }
  }

  private trait TestCase {

    private val secret  = Secret.unsafe(ByteVector.fromValidHex(List.fill(4)("caffee00").mkString))
    val hookTokenCrypto = new HookTokenCryptoImpl[Try](secret)
  }
}
