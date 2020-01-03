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

package ch.datascience.http.client

import cats.implicits._
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{Sensitive, StringTinyType, TinyTypeFactory}
import io.circe._

sealed trait AccessToken extends Any with StringTinyType with Sensitive

object AccessToken {

  final class PersonalAccessToken private (val value: String) extends AnyVal with AccessToken
  object PersonalAccessToken extends TinyTypeFactory[PersonalAccessToken](new PersonalAccessToken(_)) with NonBlank

  final class OAuthAccessToken private (val value: String) extends AnyVal with AccessToken
  object OAuthAccessToken extends TinyTypeFactory[OAuthAccessToken](new OAuthAccessToken(_)) with NonBlank

  implicit val accessTokenEncoder: Encoder[AccessToken] = {
    case OAuthAccessToken(token)    => Json.obj("oauthAccessToken"    -> Json.fromString(token))
    case PersonalAccessToken(token) => Json.obj("personalAccessToken" -> Json.fromString(token))
  }

  implicit val accessTokenDecoder: Decoder[AccessToken] = (cursor: HCursor) => {
    for {
      maybeOauth    <- cursor.downField("oauthAccessToken").as[Option[String]].flatMap(to(OAuthAccessToken.from))
      maybePersonal <- cursor.downField("personalAccessToken").as[Option[String]].flatMap(to(PersonalAccessToken.from))
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
}
