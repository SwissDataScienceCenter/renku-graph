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

package io.renku.tokenrepository.repository.creation

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}

private[tokenrepository] trait TokenValidator[F[_]] {
  def checkValid(projectId: projects.GitLabId, token: AccessToken): F[Boolean]
}

private[tokenrepository] object TokenValidator {
  def apply[F[_]: Async: GitLabClient]: F[TokenValidator[F]] = new TokenValidatorImpl[F].pure[F].widen
}

private class TokenValidatorImpl[F[_]: Async: GitLabClient] extends TokenValidator[F] {

  import io.circe.Decoder
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.implicits._

  override def checkValid(projectId: projects.GitLabId, token: AccessToken): F[Boolean] =
    GitLabClient[F].get(uri"projects" / projectId, "single-project")(mapResponse)(token.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Boolean]] = {
    case (Ok, _, resp)                               => resp.as[Boolean]
    case (Unauthorized | Forbidden | NotFound, _, _) => false.pure[F]
  }

  private implicit lazy val permissionsExistenceChecker: EntityDecoder[F, Boolean] = {
    implicit val permissionsExists: Decoder[Boolean] = Decoder.instance {
      _.downField("permissions").as[Option[Json]].map(_.isDefined)
    }
    jsonOf[F, Boolean]
  }
}
