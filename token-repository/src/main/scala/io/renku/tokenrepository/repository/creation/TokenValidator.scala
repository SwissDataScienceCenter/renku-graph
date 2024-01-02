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

package io.renku.tokenrepository.repository.creation

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.circe.Decoder.decodeOption
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.implicits._

private[tokenrepository] trait TokenValidator[F[_]] {
  def checkValid(projectId: projects.GitLabId, token: AccessToken): F[Boolean]
}

private[tokenrepository] object TokenValidator {
  def apply[F[_]: Async: GitLabClient]: F[TokenValidator[F]] =
    new TokenValidatorImpl[F](new UserIdFinderImpl[F], new MemberRightsCheckerImpl[F]).pure[F].widen
}

private class TokenValidatorImpl[F[_]: MonadThrow](userIdFinder: UserIdFinder[F],
                                                   memberRightsChecker: MemberRightsChecker[F]
) extends TokenValidator[F] {

  override def checkValid(projectId: projects.GitLabId, token: AccessToken): F[Boolean] =
    userIdFinder.findUserId(token) >>= {
      case None         => false.pure[F]
      case Some(userId) => memberRightsChecker.checkRights(projectId, userId, token)
    }
}

private trait UserIdFinder[F[_]] {
  def findUserId(token: AccessToken): F[Option[persons.GitLabId]]
}
private class UserIdFinderImpl[F[_]: Async: GitLabClient] extends UserIdFinder[F] {

  override def findUserId(token: AccessToken): F[Option[persons.GitLabId]] =
    GitLabClient[F].get(uri"user", "user")(mapUserResponse)(token.some)

  private lazy val mapUserResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[persons.GitLabId]]] = {
    case (Ok, _, resp)                               => resp.as[Option[persons.GitLabId]]
    case (Unauthorized | Forbidden | NotFound, _, _) => Option.empty[persons.GitLabId].pure[F]
  }

  private implicit lazy val userIdDecoder: EntityDecoder[F, Option[persons.GitLabId]] = {

    implicit val decoder: Decoder[Option[persons.GitLabId]] = Decoder.instance {
      import io.renku.tinytypes.json.TinyTypeDecoders.intDecoder
      _.downField("id").as[Option[persons.GitLabId]](decodeOption(intDecoder(persons.GitLabId)))
    }

    jsonOf[F, Option[persons.GitLabId]]
  }
}

private trait MemberRightsChecker[F[_]] {
  def checkRights(projectId: projects.GitLabId, userId: persons.GitLabId, token: AccessToken): F[Boolean]
}
private class MemberRightsCheckerImpl[F[_]: Async: GitLabClient] extends MemberRightsChecker[F] {

  override def checkRights(projectId: projects.GitLabId, userId: persons.GitLabId, token: AccessToken): F[Boolean] =
    GitLabClient[F].get(uri"projects" / projectId / "members" / "all" / userId, "single-project-member")(
      mapMemberResponse
    )(token.some)

  private lazy val mapMemberResponse: PartialFunction[(Status, Request[F], Response[F]), F[Boolean]] = {
    case (Ok, _, resp)                               => resp.as[Boolean]
    case (Unauthorized | Forbidden | NotFound, _, _) => false.pure[F]
  }

  private implicit lazy val roleChecker: EntityDecoder[F, Boolean] = {
    implicit val permissionsExists: Decoder[Boolean] = Decoder.instance { cur =>
      (cur.downField("access_level").as[Option[Int]] -> cur.downField("state").as[Option[String]])
        .mapN {
          case (Some(role), Some("active")) => role >= 40
          case _                            => false
        }
    }
    jsonOf[F, Boolean]
  }
}
