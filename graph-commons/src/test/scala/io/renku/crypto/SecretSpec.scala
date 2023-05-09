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

package io.renku.crypto

import com.typesafe.config.ConfigFactory
import io.renku.crypto.AesCrypto.Secret
import io.renku.generators.CommonGraphGenerators.aesCryptoSecrets
import io.renku.generators.Generators.Implicits._
import org.scalacheck.Gen
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import pureconfig.ConfigSource
import scodec.bits.Bases.Alphabets
import scodec.bits.ByteVector

class SecretSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "secretReader" should {

    val keyName = "secret"

    "decode Base64 encoded secret" in {

      val secret = aesCryptoSecrets.generateOne

      val config = ConfigFactory.parseString(s"""$keyName = "${secret.toBase64}"""")

      ConfigSource.fromConfig(config).at(keyName).load[Secret].value.toBase64 shouldBe secret.toBase64
    }

    "decode Base64 encoded secret ending with LF char" in {

      val secret = aesCryptoSecrets.generateOne

      val config = ConfigFactory.parseString(s"""$keyName = "${(secret.value :+ 10.toByte).toBase64}"""")

      ConfigSource.fromConfig(config).at(keyName).load[Secret].value.toBase64 shouldBe secret.toBase64
    }

    "fail and not print the secret if decoding fails" in {

      val value = Gen
        .listOfN(2, Gen.hexChar)
        .map(_.mkString.toLowerCase)
        .map(ByteVector.fromValidHex(_, Alphabets.HexLowercase))
        .generateOne
        .toBase64

      val config = ConfigFactory.parseString(s"""$keyName = "$value"""")

      val failure = ConfigSource.fromConfig(config).at(keyName).load[Secret].left.value.prettyPrint()

      failure should include("Cannot read AES secret")
      failure should not include value
    }
  }
}
