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

package io.renku.eventlog.events.consumers.statuschange
package projecteventstonew.cleaning

import cats.effect.Async
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.graph.webhookservice.WebhookServiceUrl
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.tokenrepository.api.TokenRepositoryClient
import org.http4s.Method.DELETE
import org.http4s.Status.{Forbidden, InternalServerError, NotFound, Ok, Unauthorized}
import org.typelevel.log4cats.Logger

private trait ProjectWebhookAndTokenRemover[F[_]] {
  def removeWebhookAndToken(project: Project): F[Unit]
}

private object ProjectWebhookAndTokenRemover {
  def apply[F[_]: Async: Logger]: F[ProjectWebhookAndTokenRemover[F]] = for {
    webhookUrl <- WebhookServiceUrl()
    trClient   <- TokenRepositoryClient[F]
  } yield new ProjectWebhookAndTokenRemoverImpl[F](webhookUrl, trClient)
}

private class ProjectWebhookAndTokenRemoverImpl[F[_]: Async: Logger](
    webhookUrl: WebhookServiceUrl,
    trClient:   TokenRepositoryClient[F]
) extends RestClient[F, ProjectWebhookAndTokenRemover[F]](Throttler.noThrottling[F])
    with ProjectWebhookAndTokenRemover[F] {

  import trClient._

  override def removeWebhookAndToken(project: Project): F[Unit] =
    findAccessToken(project.id) >>= {
      case Some(token) => removeProjectWebhook(project, token) >> removeAccessToken(project.id, maybeAccessToken = None)
      case None        => ().pure[F]
    }

  private def removeProjectWebhook(project: Project, accessToken: AccessToken): F[Unit] = for {
    validatedUrl <- validateUri(s"$webhookUrl/projects/${project.id}/webhooks")
    _            <- send(request(DELETE, validatedUrl, accessToken))(mapWebhookResponse(project))
  } yield ()

  private def mapWebhookResponse(project: Project): ResponseMapping[Unit] = {
    case (Ok | NotFound | Unauthorized | Forbidden, _, _) => ().pure[F]
    case (status @ InternalServerError, _, _) =>
      Logger[F].warn(show"$categoryName: removing webhook for project: $project got $status")
    case (status, _, _) =>
      new Exception(show"removing webhook failed with status: $status for project: $project")
        .raiseError[F, Unit]
  }
}
