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

package ch.datascience.graph.events.crypto

import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.crypto.AesCrypto
import ch.datascience.crypto.AesCrypto.Secret
import ch.datascience.graph.events.{HookAccessToken, SerializedHookAccessToken}
import javax.inject.{Inject, Singleton}
import play.api.Configuration
import pureconfig._
import pureconfig.error.ConfigReaderException

import scala.language.{higherKinds, implicitConversions}
import scala.util.control.NonFatal

class HookAccessTokenCrypto[Interpretation[_]](
    secret:    Secret
)(implicit ME: MonadError[Interpretation, Throwable])
    extends AesCrypto[Interpretation, HookAccessToken, SerializedHookAccessToken](secret) {

  override def encrypt(hookAccessToken: HookAccessToken): Interpretation[SerializedHookAccessToken] =
    for {
      encoded          <- encryptAndEncode(hookAccessToken.value)
      validatedDecoded <- validate(encoded)
    } yield validatedDecoded

  override def decrypt(serializedToken: SerializedHookAccessToken): Interpretation[HookAccessToken] = {
    for {
      decoded      <- decodeAndDecrypt(serializedToken.value)
      deserialized <- deserialize(decoded)
    } yield deserialized
  } recoverWith meaningfulError

  private def validate(value: String): Interpretation[SerializedHookAccessToken] =
    ME.fromEither[SerializedHookAccessToken] {
      SerializedHookAccessToken.from(value)
    }

  private def deserialize(value: String): Interpretation[HookAccessToken] = ME.fromEither {
    HookAccessToken.from(value)
  }

  private lazy val meaningfulError: PartialFunction[Throwable, Interpretation[HookAccessToken]] = {
    case NonFatal(cause) =>
      ME.raiseError(new RuntimeException("HookAccessToken decryption failed", cause))
  }
}

import eu.timepit.refined.pureconfig._

@Singleton
class IOHookAccessTokenCrypto(secret: Secret) extends HookAccessTokenCrypto[IO](secret) {

  @Inject def this(configuration: Configuration) = this(
    loadConfig[Secret](configuration.underlying, "event-log.hook-access-token-secret").fold(
      failures => throw new ConfigReaderException(failures),
      identity
    )
  )
}
