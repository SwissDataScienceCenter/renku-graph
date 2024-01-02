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

package io.renku.webhookservice.api

import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.control.Throttler
import io.renku.data.Message
import io.renku.data.MessageCodecs._
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.api.WebhookServiceClient.Result
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response, Status, Uri}
import org.typelevel.log4cats.Logger

trait WebhookServiceClient[F[_]] {
  def createHook(projectId: projects.GitLabId, accessToken: AccessToken): F[Result[HookCreationResult]]
}

object WebhookServiceClient {

  def apply[F[_]: Async: Logger: MetricsRegistry](config: Config): F[WebhookServiceClient[F]] =
    WebhookServiceUrl[F](config)
      .map(wsUrl => new WebhookServiceClientImpl[F](Uri.unsafeFromString(wsUrl.value)))

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

private class WebhookServiceClientImpl[F[_]: Async: Logger](wsUri: Uri)
    extends RestClient[F, Nothing](Throttler.noThrottling)
    with WebhookServiceClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  override def createHook(projectId: projects.GitLabId, accessToken: AccessToken): F[Result[HookCreationResult]] =
    send(request(POST, wsUri / "projects" / projectId / "webhooks", accessToken)) {
      case (Ok, _, _)       => Result.success(HookCreationResult.Existed.widen).pure[F]
      case (Created, _, _)  => Result.success(HookCreationResult.Created.widen).pure[F]
      case (NotFound, _, _) => Result.success(HookCreationResult.NotFound.widen).pure[F]
      case reqInfo          => toFailure[HookCreationResult]("Hook creation failed")(reqInfo)
    }

  private def toFailure[T](message: String): ((Status, Request[F], Response[F])) => F[Result[T]] = {
    case (status, req, resp) =>
      resp
        .as[Option[Message]]
        .map {
          case Some(message) => message.show
          case _ => s"$message: ${req.method} ${req.pathInfo.renderString} responded with: $status, ${resp.as[String]}"
        }
        .map(Result.failure)
  }
}
