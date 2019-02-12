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

import eu.timepit.refined.pureconfig._
import cats.MonadError
import cats.implicits._
import ch.datascience.clients.AccessToken
import ch.datascience.clients.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.crypto.AesCrypto
import ch.datascience.crypto.AesCrypto.Secret
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.SerializedAccessToken
import eu.timepit.refined.W
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.MatchesRegex
import io.circe._
import io.circe.parser._
import pureconfig._
import pureconfig.error.ConfigReaderException

import scala.language.{higherKinds, implicitConversions}
import scala.util.control.NonFatal

private class AccessTokenCrypto[Interpretation[_]](
    secret:    Secret
)(implicit ME: MonadError[Interpretation, Throwable])
    extends AesCrypto[Interpretation, AccessToken, SerializedAccessToken](secret) {

  override def encrypt(accessToken: AccessToken): Interpretation[SerializedAccessToken] =
    for {
      serializedToken  <- pure(serialize(accessToken))
      encoded          <- encryptAndEncode(serializedToken)
      validatedDecoded <- validate(encoded)
    } yield validatedDecoded

  override def decrypt(serializedToken: SerializedAccessToken): Interpretation[AccessToken] = {
    for {
      decoded      <- decodeAndDecrypt(serializedToken.value)
      deserialized <- deserialize(decoded)
    } yield deserialized
  } recoverWith meaningfulError

  private lazy val serialize: AccessToken => String = {
    case OAuthAccessToken(token)    => Json.obj("oauth"    -> Json.fromString(token)).noSpaces
    case PersonalAccessToken(token) => Json.obj("personal" -> Json.fromString(token)).noSpaces
  }

  private def validate(value: String): Interpretation[SerializedAccessToken] =
    ME.fromEither[SerializedAccessToken] {
      SerializedAccessToken.from(value)
    }

  private implicit val accessTokenDecoder: Decoder[AccessToken] = (cursor: HCursor) => {
    for {
      maybeOauth    <- cursor.downField("oauth").as[Option[String]].flatMap(to(OAuthAccessToken.from))
      maybePersonal <- cursor.downField("personal").as[Option[String]].flatMap(to(PersonalAccessToken.from))
      token <- Either.fromOption(maybeOauth orElse maybePersonal,
                                 ifNone = DecodingFailure("Access token cannot be deserialized", Nil))
    } yield token
  }

  private def to[T <: AccessToken](
      from: String => Either[IllegalArgumentException, T]
  ): Option[String] => DecodingFailure Either Option[AccessToken] = {
    case None => Right(None)
    case Some(token) =>
      from(token)
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(Option.apply)
  }

  private def deserialize(serializedToken: String): Interpretation[AccessToken] = ME.fromEither {
    parse(serializedToken)
      .flatMap(_.as[AccessToken])
  }

  private lazy val meaningfulError: PartialFunction[Throwable, Interpretation[AccessToken]] = {
    case NonFatal(cause) =>
      ME.raiseError(new RuntimeException("AccessToken decryption failed", cause))
  }
}

private object AccessTokenCrypto {

  def apply[Interpretation[_]]()(
      implicit ME: MonadError[Interpretation, Throwable]
  ): AccessTokenCrypto[Interpretation] =
    new AccessTokenCrypto[Interpretation](
      loadConfig[Secret]("projects-tokens.secret").fold(
        failures => throw new ConfigReaderException(failures),
        identity
      )
    )

  type SerializedAccessToken = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]

  object SerializedAccessToken {

    def from(value: String): Either[Throwable, SerializedAccessToken] =
      RefType
        .applyRef[SerializedAccessToken](value)
        .leftMap(_ => new IllegalArgumentException("A value to create SerializedAccessToken cannot be blank"))
  }
}
