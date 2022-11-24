/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.http.client

import cats.syntax.all._
import io.circe._
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{Sensitive, StringTinyType, TinyTypeFactory}

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

sealed trait AccessToken extends Any with StringTinyType with Sensitive

sealed trait UserAccessToken extends Any with AccessToken

sealed trait OAuthAccessToken extends Any with AccessToken

object AccessToken {

  final class PersonalAccessToken private (val value: String) extends AnyVal with UserAccessToken
  object PersonalAccessToken
      extends TinyTypeFactory[PersonalAccessToken](new PersonalAccessToken(_))
      with NonBlank[PersonalAccessToken]

  final class UserOAuthAccessToken private (val value: String) extends AnyVal with UserAccessToken with OAuthAccessToken
  object UserOAuthAccessToken
      extends TinyTypeFactory[UserOAuthAccessToken](new UserOAuthAccessToken(_))
      with NonBlank[UserOAuthAccessToken]

  final class ProjectAccessToken private (val value: String) extends AnyVal with AccessToken with OAuthAccessToken
  object ProjectAccessToken
      extends TinyTypeFactory[ProjectAccessToken](new ProjectAccessToken(_))
      with NonBlank[ProjectAccessToken]

  private val base64Decoder = Base64.getDecoder
  private val base64Encoder = Base64.getEncoder

  implicit val accessTokenEncoder: Encoder[AccessToken] = {
    case ProjectAccessToken(token) =>
      Json.obj("projectAccessToken" -> Json.fromString(new String(base64Encoder.encode(token.getBytes(UTF_8)), UTF_8)))
    case UserOAuthAccessToken(token) =>
      Json.obj(
        "userOAuthAccessToken" -> Json.fromString(new String(base64Encoder.encode(token.getBytes(UTF_8)), UTF_8))
      )
    case PersonalAccessToken(token) =>
      Json.obj("personalAccessToken" -> Json.fromString(new String(base64Encoder.encode(token.getBytes(UTF_8)), UTF_8)))
  }

  implicit val accessTokenDecoder: Decoder[AccessToken] = cursor => {

    def maybeExtract(field: String, as: String => Either[IllegalArgumentException, AccessToken]) =
      cursor.downField(field).as[Option[String]].flatMap(to(as))

    maybeExtract("projectAccessToken", as = ProjectAccessToken.from)
      .flatMap {
        case token @ Some(_) => token.asRight
        case None            => maybeExtract("userOAuthAccessToken", as = UserOAuthAccessToken.from)
      }
      .flatMap {
        case token @ Some(_) => token.asRight
        case None            => maybeExtract("personalAccessToken", as = PersonalAccessToken.from)
      }
      .flatMap {
        case Some(token) => token.asRight
        case None        => DecodingFailure("Access token cannot be deserialized", Nil).asLeft
      }
  }

  private def to[T <: AccessToken](
      from: String => Either[IllegalArgumentException, T]
  ): Option[String] => Either[DecodingFailure, Option[AccessToken]] = {
    case None => Right(None)
    case Some(token) =>
      from(new String(base64Decoder.decode(token.getBytes(UTF_8)), UTF_8))
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(Option.apply)
  }
}
