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

package ch.datascience.webhookservice.crypto

import eu.timepit.refined.pureconfig._
import java.util.Base64

import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.crypto.HookTokenCrypto.{Secret, SerializedHookToken}
import ch.datascience.webhookservice.model.{HookAccessToken, HookToken}
import eu.timepit.refined.W
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.MinSize
import eu.timepit.refined.string.MatchesRegex
import io.circe.parser._
import io.circe.{Decoder, HCursor, Json}
import javax.crypto.Cipher
import javax.crypto.Cipher.{DECRYPT_MODE, ENCRYPT_MODE}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.inject.{Inject, Singleton}
import play.api.Configuration
import pureconfig._
import pureconfig.error.ConfigReaderException

import scala.language.{higherKinds, implicitConversions}
import scala.util.Try
import scala.util.control.NonFatal

class HookTokenCrypto[Interpretation[_]](secret: Secret)(implicit ME: MonadError[Interpretation, Throwable]) {

  private lazy val base64Decoder    = Base64.getDecoder
  private lazy val base64Encoder    = Base64.getEncoder
  private lazy val algorithm        = "AES/CBC/PKCS5Padding"
  private lazy val key              = new SecretKeySpec(base64Decoder.decode(secret.value), "AES")
  private lazy val ivSpec           = new IvParameterSpec(new Array[Byte](16))
  private lazy val charset          = "utf-8"
  private lazy val encryptingCipher = cipher(ENCRYPT_MODE)
  private lazy val decryptingCipher = cipher(DECRYPT_MODE)

  def encrypt(hookToken: HookToken): Interpretation[SerializedHookToken] =
    for {
      serializedToken  <- serialize(hookToken)
      encoded          <- encryptAndEncode(serializedToken)
      validatedDecoded <- validate(encoded)
    } yield validatedDecoded

  def decrypt(serializedToken: SerializedHookToken): Interpretation[HookToken] = {
    for {
      decoded      <- decodeAndDecrypt(serializedToken)
      deserialized <- deserialize(decoded)
    } yield deserialized
  } recoverWith meaningfulError

  private def serialize(hook: HookToken): Interpretation[String] = pure {
    Json
      .obj(
        "projectId"       -> Json.fromInt(hook.projectId.value),
        "hookAccessToken" -> Json.fromString(hook.hookAccessToken.value)
      )
      .noSpaces
  }

  private def validate(value: String): Interpretation[SerializedHookToken] =
    ME.fromEither[SerializedHookToken] {
      SerializedHookToken.from(value)
    }

  private def cipher(mode: Int): Cipher = {
    val c = Cipher.getInstance(algorithm)
    c.init(mode, key, ivSpec)
    c
  }

  private def encryptAndEncode(authToken: String): Interpretation[String] =
    new String(
      base64Encoder.encode(encryptingCipher.doFinal(authToken.getBytes(charset))),
      charset
    )

  private def decodeAndDecrypt(authToken: SerializedHookToken): Interpretation[String] =
    new String(
      decryptingCipher.doFinal(base64Decoder.decode(authToken.value.getBytes(charset))),
      charset
    )

  private implicit val hookTokenDecoder: Decoder[HookToken] = (cursor: HCursor) =>
    for {
      projectId       <- cursor.downField("projectId").as[ProjectId]
      hookAccessToken <- cursor.downField("hookAccessToken").as[HookAccessToken]
    } yield HookToken(projectId, hookAccessToken)

  private def deserialize(json: String): Interpretation[HookToken] = ME.fromEither {
    parse(json)
      .flatMap(_.as[HookToken])
  }

  private lazy val meaningfulError: PartialFunction[Throwable, Interpretation[HookToken]] = {
    case NonFatal(cause) =>
      ME.raiseError(new RuntimeException("HookToken decryption failed", cause))
  }

  private implicit def pure[T](value: => T): Interpretation[T] =
    Try(value)
      .fold(
        ME.raiseError,
        ME.pure
      )
}

object HookTokenCrypto {
  type Secret              = String Refined MinSize[W.`16`.T]
  type SerializedHookToken = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]

  object SerializedHookToken {

    def from(value: String): Either[Throwable, SerializedHookToken] =
      RefType
        .applyRef[SerializedHookToken](value)
        .leftMap(_ => new IllegalArgumentException("A value to create HookAuthToken cannot be blank"))
  }
}

@Singleton
class IOHookTokenCrypto(secret: Secret) extends HookTokenCrypto[IO](secret) {

  @Inject def this(configuration: Configuration) = this(
    loadConfig[Secret](configuration.underlying, "services.gitlab.secret-token-secret").fold(
      failures => throw new ConfigReaderException(failures),
      identity
    )
  )
}
