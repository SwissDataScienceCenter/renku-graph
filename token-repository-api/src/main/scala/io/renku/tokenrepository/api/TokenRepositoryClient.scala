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

package io.renku.tokenrepository.api

import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Decoder
import io.circe.syntax._
import io.renku.control.Throttler
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.{AccessToken, GitLabClient, RestClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Uri
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait TokenRepositoryClient[F[_]] {
  def findAccessToken(projectId:   projects.GitLabId): F[Option[AccessToken]]
  def findAccessToken(projectSlug: projects.Slug): F[Option[AccessToken]]
  def removeAccessToken(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[Unit]
  def storeAccessToken(projectId:  GitLabId, accessToken:      AccessToken):         F[Unit]
}

object TokenRepositoryClient {

  def apply[F[_]: Async: Logger]: F[TokenRepositoryClient[F]] = apply()

  def apply[F[_]: Async: Logger](config: Config = ConfigFactory.load): F[TokenRepositoryClient[F]] =
    TokenRepositoryUrl[F](config)
      .map(url => new TokenRepositoryClientImpl[F](Uri.unsafeFromString(url.value)))
}

private class TokenRepositoryClientImpl[F[_]: Async: Logger](trUri: Uri)
    extends RestClient[F, Nothing](Throttler.noThrottling)
    with TokenRepositoryClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F]
    with RenkuEntityCodec {

  override def findAccessToken(projectId: projects.GitLabId): F[Option[AccessToken]] =
    findAccessToken(trUri / "projects" / projectId / "tokens")

  override def findAccessToken(projectSlug: projects.Slug): F[Option[AccessToken]] =
    findAccessToken(trUri / "projects" / projectSlug / "tokens")

  private def findAccessToken(uri: Uri): F[Option[AccessToken]] =
    send(GET(uri)) {
      case (Ok, _, response) => response.asJson(Decoder.decodeOption[AccessToken])
      case (NotFound, _, _)  => Option.empty[AccessToken].pure[F]
    }

  override def removeAccessToken(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[Unit] = {
    val req = GitLabClient.request[F](DELETE, trUri / "projects" / projectId / "tokens", maybeAccessToken)
    send(req) { case (NoContent, _, _) => ().pure[F] }
  }

  override def storeAccessToken(projectId: GitLabId, accessToken: AccessToken): F[Unit] =
    send(
      POST(trUri / "projects" / projectId / "tokens").withEntity(accessToken.asJson)
    ) { case (NoContent, _, _) => ().pure[F] }
}
