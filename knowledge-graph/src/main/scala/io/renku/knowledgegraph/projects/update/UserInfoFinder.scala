/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.update

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.core.client.UserInfo
import io.renku.http.client.{GitLabClient, UserAccessToken}
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status.{NotFound, Ok}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.implicits._
import org.http4s.{Request, Response, Status}

private trait UserInfoFinder[F[_]] {
  def findUserInfo(generateOne: UserAccessToken): F[Option[UserInfo]]
}

private object UserInfoFinder {
  def apply[F[_]: Async: GitLabClient]: UserInfoFinder[F] = new UserInfoFinderImpl[F]
}

private class UserInfoFinderImpl[F[_]: Async: GitLabClient] extends UserInfoFinder[F] {

  override def findUserInfo(at: UserAccessToken): F[Option[UserInfo]] =
    GitLabClient[F]
      .get(uri"user", "user")(mapResponse)(at.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[UserInfo]]] = {
    case (Ok, _, resp)    => resp.as[Option[UserInfo]]
    case (NotFound, _, _) => Option.empty[UserInfo].pure[F]
  }

  private implicit lazy val infoDecoder: Decoder[UserInfo] =
    Decoder.forProduct2("name", "email")(UserInfo.apply)
}
