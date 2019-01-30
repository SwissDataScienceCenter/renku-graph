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

package ch.datascience.crypto

import java.util.Base64

import cats.MonadError
import ch.datascience.crypto.AesCrypto.Secret
import ch.datascience.graph.events.crypto.HookAccessTokenCrypto.SerializedHookAccessToken
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.MinSize
import javax.crypto.Cipher
import javax.crypto.Cipher.{DECRYPT_MODE, ENCRYPT_MODE}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import scala.language.higherKinds
import scala.util.Try

abstract class AesCrypto[Interpretation[_], NONENCRYPTED, ENCRYPTED](
    secret:    Secret
)(implicit ME: MonadError[Interpretation, Throwable]) {

  private lazy val base64Decoder    = Base64.getDecoder
  private lazy val base64Encoder    = Base64.getEncoder
  private lazy val algorithm        = "AES/CBC/PKCS5Padding"
  private lazy val key              = new SecretKeySpec(base64Decoder.decode(secret.value), "AES")
  private lazy val ivSpec           = new IvParameterSpec(new Array[Byte](16))
  private lazy val charset          = "utf-8"
  private lazy val encryptingCipher = cipher(ENCRYPT_MODE)
  private lazy val decryptingCipher = cipher(DECRYPT_MODE)

  def encrypt(nonEncrypted: NONENCRYPTED): Interpretation[ENCRYPTED]
  def decrypt(encrypted:    ENCRYPTED):    Interpretation[NONENCRYPTED]

  private def cipher(mode: Int): Cipher = {
    val c = Cipher.getInstance(algorithm)
    c.init(mode, key, ivSpec)
    c
  }

  protected def encryptAndEncode(serializedToken: String): Interpretation[String] = pure {
    new String(
      base64Encoder.encode(encryptingCipher.doFinal(serializedToken.getBytes(charset))),
      charset
    )
  }

  protected def decodeAndDecrypt(authToken: SerializedHookAccessToken): Interpretation[String] = pure {
    new String(
      decryptingCipher.doFinal(base64Decoder.decode(authToken.value.getBytes(charset))),
      charset
    )
  }

  protected def pure[T](value: => T): Interpretation[T] = ME.fromTry {
    Try(value)
  }
}

object AesCrypto {
  type Secret = String Refined MinSize[W.`16`.T]
}
