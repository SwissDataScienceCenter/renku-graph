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

package io.renku.eventlog.events.categories.statuschange
package projectCleaner

import cats.effect.Async
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.graph.tokenrepository.{AccessTokenFinder, TokenRepositoryUrl}
import io.renku.graph.webhookservice.WebhookServiceUrl
import io.renku.http.client.{AccessToken, RestClient}
import org.http4s.EntityDecoder
import org.http4s.Method.DELETE
import org.http4s.Status.{Forbidden, InternalServerError, NoContent, NotFound, Ok, Unauthorized}
import org.http4s.circe.jsonOf
import org.typelevel.log4cats.Logger

private trait ProjectWebhookAndTokenRemover[F[_]] {
  def removeWebhookAndToken(project: Project): F[Unit]
}

private object ProjectWebhookAndTokenRemover {
  def apply[F[_]: Async: Logger](): F[ProjectWebhookAndTokenRemover[F]] = for {
    accessTokenFinder  <- AccessTokenFinder[F]
    webhookUrl         <- WebhookServiceUrl()
    tokenRepositoryUrl <- TokenRepositoryUrl()
  } yield new ProjectWebhookAndTokenRemoverImpl[F](accessTokenFinder, webhookUrl, tokenRepositoryUrl)
}

private class ProjectWebhookAndTokenRemoverImpl[F[_]: Async: Logger](accessTokenFinder: AccessTokenFinder[F],
                                                                     webhookUrl:         WebhookServiceUrl,
                                                                     tokenRepositoryUrl: TokenRepositoryUrl
) extends RestClient[F, ProjectWebhookAndTokenRemover[F]](Throttler.noThrottling[F])
    with ProjectWebhookAndTokenRemover[F] {

  import AccessTokenFinder.projectIdToPath

  override def removeWebhookAndToken(project: Project): F[Unit] =
    accessTokenFinder.findAccessToken(project.id) >>= {
      case Some(token) => removeProjectWebhook(project, token) >> removeProjectTokens(project)
      case None        => ().pure[F]
    }

  private def removeProjectWebhook(project: Project, accessToken: AccessToken): F[Unit] = for {
    validatedUrl <- validateUri(s"$webhookUrl/projects/${project.id}/webhooks")
    _            <- send(request(DELETE, validatedUrl, accessToken))(mapWebhookResponse(project))
  } yield ()

  private def removeProjectTokens(project: Project): F[Unit] = for {
    validatedUrl <- validateUri(s"$tokenRepositoryUrl/projects/${project.id}/tokens")
    _            <- send(request(DELETE, validatedUrl))(mapTokenRepoResponse(project))
  } yield ()

  private implicit val tokenEntityDecoder: EntityDecoder[F, AccessToken] = jsonOf[F, AccessToken]

  private def mapWebhookResponse(project: Project): ResponseMapping[Unit] = {
    case (Ok | NotFound | Unauthorized | Forbidden, _, _) => ().pure[F]
    case (status @ InternalServerError, _, _) =>
      Logger[F].warn(show"$categoryName: removing webhook for project: $project got $status")
    case (status, _, _) =>
      new Exception(show"removing webhook failed with status: $status for project: $project")
        .raiseError[F, Unit]
  }

  private def mapTokenRepoResponse(project: Project): ResponseMapping[Unit] = {
    case (NoContent | NotFound, _, _) => ().pure[F]
    case (status, _, _) =>
      new Exception(s"Removing project token failed with status: $status for project: ${project.show}")
        .raiseError[F, Unit]
  }
}
