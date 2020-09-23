/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import cats.MonadError
import ch.datascience.crypto.AesCrypto.Secret
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.MinSize
import javax.crypto.Cipher
import javax.crypto.Cipher.{DECRYPT_MODE, ENCRYPT_MODE}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import scala.util.Try

abstract class AesCrypto[Interpretation[_], NONENCRYPTED, ENCRYPTED](
    secret:    Secret
)(implicit ME: MonadError[Interpretation, Throwable]) {

  private val base64Decoder    = Base64.getDecoder
  private val base64Encoder    = Base64.getEncoder
  private val algorithm        = "AES/CBC/PKCS5Padding"
  private val key              = new SecretKeySpec(base64Decoder.decode(secret.value).takeWhile(_ != 10), "AES")
  private val ivSpec           = new IvParameterSpec(new Array[Byte](16))
  private val encryptingCipher = cipher(ENCRYPT_MODE)
  private val decryptingCipher = cipher(DECRYPT_MODE)

  def encrypt(nonEncrypted: NONENCRYPTED): Interpretation[ENCRYPTED]
  def decrypt(encrypted:    ENCRYPTED):    Interpretation[NONENCRYPTED]

  private def cipher(mode: Int): Cipher = {
    val c = Cipher.getInstance(algorithm)
    c.init(mode, key, ivSpec)
    c
  }

  protected def encryptAndEncode(toEncryptAndEncode: String): Interpretation[String] = pure {
    new String(
      base64Encoder.encode(encryptingCipher.doFinal(toEncryptAndEncode.getBytes(UTF_8))),
      UTF_8
    )
  }

  protected def decodeAndDecrypt(toDecodeAndDecrypt: String): Interpretation[String] = pure {
    new String(
      decryptingCipher.doFinal(base64Decoder.decode(toDecodeAndDecrypt.getBytes(UTF_8))),
      UTF_8
    )
  }

  protected def pure[T](value: => T): Interpretation[T] = ME.fromTry {
    Try(value)
  }
}

object AesCrypto {
  type Secret = String Refined MinSize[W.`16`.T]
}
