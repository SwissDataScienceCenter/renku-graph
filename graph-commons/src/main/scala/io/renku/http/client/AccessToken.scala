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

object AccessToken {

  final class PersonalAccessToken private (val value: String) extends AnyVal with AccessToken
  object PersonalAccessToken
      extends TinyTypeFactory[PersonalAccessToken](new PersonalAccessToken(_))
      with NonBlank[PersonalAccessToken]

  final class OAuthAccessToken private (val value: String) extends AnyVal with AccessToken
  object OAuthAccessToken
      extends TinyTypeFactory[OAuthAccessToken](new OAuthAccessToken(_))
      with NonBlank[OAuthAccessToken]

  private val base64Decoder = Base64.getDecoder
  private val base64Encoder = Base64.getEncoder

  implicit val accessTokenEncoder: Encoder[AccessToken] = {
    case OAuthAccessToken(token) =>
      Json.obj("oauthAccessToken" -> Json.fromString(new String(base64Encoder.encode(token.getBytes(UTF_8)), UTF_8)))
    case PersonalAccessToken(token) =>
      Json.obj("personalAccessToken" -> Json.fromString(new String(base64Encoder.encode(token.getBytes(UTF_8)), UTF_8)))
  }

  implicit val accessTokenDecoder: Decoder[AccessToken] = (cursor: HCursor) =>
    for {
      maybeOauth    <- cursor.downField("oauthAccessToken").as[Option[String]].flatMap(to(OAuthAccessToken.from))
      maybePersonal <- cursor.downField("personalAccessToken").as[Option[String]].flatMap(to(PersonalAccessToken.from))
      token <- Either.fromOption(maybeOauth orElse maybePersonal,
                                 ifNone = DecodingFailure("Access token cannot be deserialized", Nil)
               )
    } yield token

  private def to[T <: AccessToken](
      from: String => Either[IllegalArgumentException, T]
  ): Option[String] => DecodingFailure Either Option[AccessToken] = {
    case None => Right(None)
    case Some(token) =>
      from(new String(base64Decoder.decode(token.getBytes(UTF_8)), UTF_8))
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(Option.apply)
  }
}
