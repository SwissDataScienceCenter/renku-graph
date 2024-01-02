/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
import cats.syntax.all._
import io.renku.config.ConfigLoader.{asciiByteVectorReader, base64ByteVectorReader}
import io.renku.crypto.AesCrypto.Secret
import pureconfig.ConfigReader
import pureconfig.error.FailureReason
import scodec.bits.ByteVector

import java.nio.charset.CharacterCodingException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.Cipher.{DECRYPT_MODE, ENCRYPT_MODE}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

abstract class AesCrypto[F[_]: MonadThrow, NONENCRYPTED, ENCRYPTED](secret: Secret) {

  private val base64Decoder = Base64.getDecoder
  private val base64Encoder = Base64.getEncoder
  private val algorithm     = "AES/CBC/PKCS5Padding"
  private val ivSpec        = new IvParameterSpec(new Array[Byte](16))

  def encrypt(nonEncrypted: NONENCRYPTED): F[ENCRYPTED]
  def decrypt(encrypted:    ENCRYPTED):    F[NONENCRYPTED]

  private def cipher(mode: Int): Cipher = {
    val c = Cipher.getInstance(algorithm)
    c.init(mode, new SecretKeySpec(secret.value.toArray, "AES"), ivSpec)
    c
  }

  protected def encryptAndEncode(toEncryptAndEncode: String): F[String] = MonadThrow[F].catchNonFatal {
    base64Encoder.encodeToString(cipher(ENCRYPT_MODE).doFinal(toEncryptAndEncode.getBytes(UTF_8)))
  }

  protected def decodeAndDecrypt(toDecodeAndDecrypt: String): F[String] = MonadThrow[F].catchNonFatal {
    new String(
      cipher(DECRYPT_MODE).doFinal(base64Decoder.decode(toDecodeAndDecrypt.getBytes(UTF_8))),
      UTF_8
    )
  }
}

object AesCrypto {

  final class Secret private (val value: ByteVector) extends AnyVal {
    override def toString: String                                   = "<sensitive>"
    def decodeAscii:       Either[CharacterCodingException, String] = value.decodeAscii
  }

  object Secret {

    def apply(value: ByteVector): Either[String, Secret] = {
      val expectedLengths = List(16L, 24L, 32L)
      if (expectedLengths contains value.length) Right(new Secret(value))
      else Left(s"Expected ${expectedLengths.mkString(" or ")} bytes, but got ${value.length}")
    }

    def unsafe(value: ByteVector): Secret = apply(value).fold(sys.error, identity)

    private def fromBase64(b64: String): Either[String, Secret] =
      ByteVector.fromBase64Descriptive(b64).flatMap(apply)

    def unsafeFromBase64(b64: String): Secret =
      fromBase64(b64).fold(sys.error, identity)

    implicit def secretReader: ConfigReader[Secret] =
      base64SecretReader orElse asciiSecretReader

    private lazy val base64SecretReader =
      base64ByteVectorReader.emap { bv =>
        Secret(bv.takeWhile(_ != 10))
          .leftMap(err => new FailureReason { override lazy val description: String = s"Cannot read AES secret: $err" })
      }

    private lazy val asciiSecretReader =
      asciiByteVectorReader.emap { bv =>
        Secret(bv).leftMap(err =>
          new FailureReason { override lazy val description: String = s"Cannot read AES secret: $err" }
        )
      }
  }
}
