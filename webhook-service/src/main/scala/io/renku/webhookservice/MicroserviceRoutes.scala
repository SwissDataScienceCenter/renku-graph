/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice

import cats.MonadThrow
import cats.effect.{Async, Clock, Resource}
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.http.server.binders.ProjectId
import io.renku.graph.http.server.security.GitLabAuthenticator
import io.renku.http.server.security.Authentication
import io.renku.http.server.security.model.AuthUser
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.eventprocessing.{HookEventEndpoint, ProcessingStatusEndpoint}
import io.renku.webhookservice.hookcreation.HookCreationEndpoint
import io.renku.webhookservice.hookvalidation.HookValidationEndpoint
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.AuthedRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.AuthMiddleware
import org.typelevel.log4cats.Logger

private class MicroserviceRoutes[F[_]: MonadThrow](
    hookEventEndpoint:        HookEventEndpoint[F],
    hookCreationEndpoint:     HookCreationEndpoint[F],
    hookValidationEndpoint:   HookValidationEndpoint[F],
    processingStatusEndpoint: ProcessingStatusEndpoint[F],
    authMiddleware:           AuthMiddleware[F, AuthUser],
    routesMetrics:            RoutesMetrics[F]
)(implicit clock:             Clock[F])
    extends Http4sDsl[F] {

  import hookCreationEndpoint._
  import hookEventEndpoint._
  import hookValidationEndpoint._
  import org.http4s.HttpRoutes
  import processingStatusEndpoint._
  import routesMetrics._

  // format: off
  private lazy val authorizedRoutes: HttpRoutes[F] = authMiddleware {
    AuthedRoutes.of {
      case POST -> Root / "projects" / ProjectId(projectId) / "webhooks" as authUser                => createHook(projectId, authUser)
      case POST -> Root / "projects" / ProjectId(projectId) / "webhooks" / "validation" as authUser => validateHook(projectId, authUser)
    }
  }

  lazy val nonAuthorizedRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case           GET  -> Root / "ping"                                                        => Ok("pong")
    case request @ POST -> Root / "webhooks" / "events"                                         => processPushEvent(request)
    case           GET  -> Root / "projects" / ProjectId(projectId) / "events" / "status"       => fetchProcessingStatus(projectId)
  }
  // format: on

  lazy val routes: Resource[F, HttpRoutes[F]] = (nonAuthorizedRoutes <+> authorizedRoutes).withMetrics
}

private object MicroserviceRoutes {
  def apply[F[_]: Async: Logger](
      metricsRegistry:       MetricsRegistry[F],
      gitLabThrottler:       Throttler[F, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[F]
  ): F[MicroserviceRoutes[F]] = for {
    projectHookUrl      <- ProjectHookUrl.fromConfig[F]()
    hookTokenCrypto     <- HookTokenCrypto[F]()
    hookEventEndpoint   <- HookEventEndpoint(hookTokenCrypto)
    hookCreatorEndpoint <- HookCreationEndpoint(projectHookUrl, gitLabThrottler, hookTokenCrypto)
    processingStatusEndpoint <-
      eventprocessing.ProcessingStatusEndpoint(projectHookUrl, gitLabThrottler, executionTimeRecorder)
    hookValidationEndpoint <- HookValidationEndpoint(projectHookUrl, gitLabThrottler)
    authenticator          <- GitLabAuthenticator(gitLabThrottler)
    authMiddleware         <- Authentication.middleware(authenticator)
  } yield new MicroserviceRoutes[F](
    hookEventEndpoint,
    hookCreatorEndpoint,
    hookValidationEndpoint,
    processingStatusEndpoint,
    authMiddleware,
    new RoutesMetrics[F](metricsRegistry)
  )
}
