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

package ch.datascience.webhookservice.crypto

import cats.MonadError
import cats.syntax.all._
import ch.datascience.crypto.AesCrypto
import ch.datascience.crypto.AesCrypto.Secret
import ch.datascience.graph.model.projects.Id
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.model.HookToken
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.W
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.pureconfig._
import eu.timepit.refined.string.MatchesRegex
import io.circe.parser._
import io.circe.{Decoder, HCursor, Json}

import scala.language.implicitConversions
import scala.util.control.NonFatal

class HookTokenCrypto[Interpretation[_]](
    secret:    Secret
)(implicit ME: MonadError[Interpretation, Throwable])
    extends AesCrypto[Interpretation, HookToken, SerializedHookToken](secret) {

  override def encrypt(hookToken: HookToken): Interpretation[SerializedHookToken] =
    for {
      serializedToken  <- serialize(hookToken)
      encoded          <- encryptAndEncode(serializedToken)
      validatedDecoded <- validate(encoded)
    } yield validatedDecoded

  override def decrypt(serializedToken: SerializedHookToken): Interpretation[HookToken] = {
    for {
      decoded      <- decodeAndDecrypt(serializedToken.value)
      deserialized <- deserialize(decoded)
    } yield deserialized
  } recoverWith meaningfulError

  private def serialize(hook: HookToken): Interpretation[String] = pure {
    Json.obj("projectId" -> Json.fromInt(hook.projectId.value)).noSpaces
  }

  private def validate(value: String): Interpretation[SerializedHookToken] =
    ME.fromEither[SerializedHookToken] {
      SerializedHookToken.from(value)
    }

  private implicit val hookTokenDecoder: Decoder[HookToken] = (cursor: HCursor) =>
    cursor.downField("projectId").as[Id].map(HookToken)

  private def deserialize(json: String): Interpretation[HookToken] = ME.fromEither {
    parse(json)
      .flatMap(_.as[HookToken])
  }

  private lazy val meaningfulError: PartialFunction[Throwable, Interpretation[HookToken]] = { case NonFatal(cause) =>
    ME.raiseError(new RuntimeException("HookToken decryption failed", cause))
  }
}

object HookTokenCrypto {
  import ch.datascience.config.ConfigLoader._

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load()
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[HookTokenCrypto[Interpretation]] =
    find[Interpretation, Secret]("services.gitlab.hook-token-secret", config)
      .map(new HookTokenCrypto[Interpretation](_))

  type SerializedHookToken = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]

  object SerializedHookToken {

    def from(value: String): Either[Throwable, SerializedHookToken] =
      RefType
        .applyRef[SerializedHookToken](value)
        .leftMap(_ => new IllegalArgumentException("A value to create HookToken cannot be blank"))
  }
}
