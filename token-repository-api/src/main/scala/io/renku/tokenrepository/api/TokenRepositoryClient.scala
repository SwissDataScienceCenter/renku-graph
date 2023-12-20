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

package io.renku.tokenrepository.api

import TokenRepositoryClient.Result
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.control.Throttler
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.metrics.MetricsRegistry
import org.http4s.Uri
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait TokenRepositoryClient[F[_]] {
  def findAccessToken(projectId:   projects.GitLabId): F[Result[Option[AccessToken]]]
  def findAccessToken(projectSlug: projects.Slug):     F[Result[Option[AccessToken]]]
}

object TokenRepositoryClient {

  def apply[F[_]: Async: Logger: MetricsRegistry]: F[TokenRepositoryClient[F]] = apply()

  def apply[F[_]: Async: Logger: MetricsRegistry](config: Config = ConfigFactory.load): F[TokenRepositoryClient[F]] =
    TokenRepositoryUrl[F](config)
      .map(url => new TokenRepositoryClientImpl[F](Uri.unsafeFromString(url.value)))

  sealed trait Result[+A] {
    def toEither: Either[Throwable, A]
  }

  object Result {
    final case class Success[+A](value: A) extends Result[A] {
      def toEither: Either[Throwable, A] = Right(value)
    }

    final case class Failure(error: String) extends RuntimeException(error) with Result[Nothing] {
      def toEither: Either[Throwable, Nothing] = Left(this)
    }

    def success[A](value: A): Result[A] = Success(value)

    def failure[A](error: String): Result[A] = Failure(error)
  }
}

private class TokenRepositoryClientImpl[F[_]: Async: Logger](trUri: Uri)
    extends RestClient[F, Nothing](Throttler.noThrottling)
    with TokenRepositoryClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  override def findAccessToken(projectId: projects.GitLabId): F[Result[Option[AccessToken]]] =
    findAccessToken(trUri / "projects" / projectId / "tokens")

  override def findAccessToken(projectSlug: projects.Slug): F[Result[Option[AccessToken]]] =
    findAccessToken(trUri / "projects" / projectSlug / "tokens")

  private def findAccessToken(uri: Uri): F[Result[Option[AccessToken]]] =
    send(GET(uri)) {
      case (Ok, _, response) => response.as[Option[AccessToken]].map(Result.success)
      case (NotFound, _, _)  => Result.success(Option.empty[AccessToken]).pure[F]
      case (status, req, _) =>
        Result
          .failure[Option[AccessToken]](s"Finding project access token failed: ${req.pathInfo.renderString}: $status")
          .pure[F]
    }
}
