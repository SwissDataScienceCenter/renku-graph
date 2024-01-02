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

package io.renku.tokenrepository.repository

import AccessTokenCrypto.EncryptedAccessToken
import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.W
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.MatchesRegex
import io.circe._
import io.circe.parser._
import io.renku.crypto.AesCrypto
import io.renku.crypto.AesCrypto.Secret
import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken._

import scala.util.control.NonFatal

private trait AccessTokenCrypto[F[_]] {
  def encrypt(accessToken:    AccessToken):          F[EncryptedAccessToken]
  def decrypt(encryptedToken: EncryptedAccessToken): F[AccessToken]
}

private class AccessTokenCryptoImpl[F[_]: MonadThrow](
    secret: Secret
) extends AesCrypto[F, AccessToken, EncryptedAccessToken](secret)
    with AccessTokenCrypto[F] {

  override def encrypt(accessToken: AccessToken): F[EncryptedAccessToken] = for {
    serializedToken  <- serialize(accessToken).pure[F]
    encoded          <- encryptAndEncode(serializedToken)
    validatedDecoded <- validate(encoded)
  } yield validatedDecoded

  override def decrypt(encryptedToken: EncryptedAccessToken): F[AccessToken] = {
    for {
      decoded      <- decodeAndDecrypt(encryptedToken.value)
      deserialized <- deserialize(decoded)
    } yield deserialized
  } recoverWith meaningfulError

  private lazy val serialize: AccessToken => String = {
    case ProjectAccessToken(token)   => Json.obj("project" -> Json.fromString(token)).noSpaces
    case UserOAuthAccessToken(token) => Json.obj("oauth" -> Json.fromString(token)).noSpaces
    case PersonalAccessToken(token)  => Json.obj("personal" -> Json.fromString(token)).noSpaces
  }

  private def validate(value: String): F[EncryptedAccessToken] =
    MonadThrow[F].fromEither[EncryptedAccessToken] {
      EncryptedAccessToken.from(value)
    }

  private implicit val accessTokenDecoder: Decoder[AccessToken] = cursor => {

    def maybeExtract(field: String, as: String => Either[IllegalArgumentException, AccessToken]) =
      cursor.downField(field).as[Option[String]].flatMap(to(as))

    maybeExtract("project", as = ProjectAccessToken.from)
      .flatMap {
        case token @ Some(_) => token.asRight
        case None            => maybeExtract("oauth", as = UserOAuthAccessToken.from)
      }
      .flatMap {
        case token @ Some(_) => token.asRight
        case None            => maybeExtract("personal", as = PersonalAccessToken.from)
      }
      .flatMap {
        case Some(token) => token.asRight
        case None        => DecodingFailure("Access token cannot be deserialized", Nil).asLeft
      }
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

  private def deserialize(serializedToken: String): F[AccessToken] =
    MonadThrow[F].fromEither {
      parse(serializedToken).flatMap(_.as[AccessToken])
    }

  private lazy val meaningfulError: PartialFunction[Throwable, F[AccessToken]] = { case NonFatal(cause) =>
    MonadThrow[F].raiseError(new RuntimeException("AccessToken decryption failed", cause))
  }
}

private object AccessTokenCrypto {

  import io.renku.config.ConfigLoader._

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load()): F[AccessTokenCrypto[F]] = for {
    secret <- find[F, Secret]("projects-tokens.secret", config)
  } yield new AccessTokenCryptoImpl[F](secret)

  type EncryptedAccessToken = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]

  object EncryptedAccessToken {

    def from(value: String): Either[Throwable, EncryptedAccessToken] =
      RefType
        .applyRef[EncryptedAccessToken](value)
        .leftMap(_ => new IllegalArgumentException("A value to create SerializedAccessToken cannot be blank"))
  }
}
