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

package io.renku.webhookservice

import cats.MonadThrow
import cats.effect.{Async, Resource}
import cats.syntax.all._
import io.renku.graph.http.server.binders.ProjectId
import io.renku.graph.http.server.security.GitLabAuthenticator
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.Authentication
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.version
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.hookcreation.HookCreationEndpoint
import io.renku.webhookservice.hookdeletion.HookDeletionEndpoint
import io.renku.webhookservice.hookvalidation.HookValidationEndpoint
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.AuthedRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.AuthMiddleware
import org.typelevel.log4cats.Logger

private class MicroserviceRoutes[F[_]: MonadThrow](
    webhookEventsEndpoint:  webhookevents.Endpoint[F],
    hookCreationEndpoint:   HookCreationEndpoint[F],
    hookValidationEndpoint: HookValidationEndpoint[F],
    hookDeletionEndpoint:   HookDeletionEndpoint[F],
    eventStatusEndpoint:    eventstatus.Endpoint[F],
    authMiddleware:         AuthMiddleware[F, AuthUser],
    optAuthMiddleware:      AuthMiddleware[F, Option[AuthUser]],
    routesMetrics:          RoutesMetrics[F],
    versionRoutes:          version.Routes[F]
) extends Http4sDsl[F] {

  import hookCreationEndpoint._
  import hookDeletionEndpoint._
  import webhookEventsEndpoint._
  import hookValidationEndpoint._
  import org.http4s.HttpRoutes
  import eventStatusEndpoint._
  import routesMetrics._

  // format: off
  private lazy val authorizedRoutes: HttpRoutes[F] = authMiddleware {
    AuthedRoutes.of {
      case POST   -> Root / "projects" / ProjectId(projectId) / "webhooks" as authUser                => createHook(projectId, authUser)
      case DELETE -> Root / "projects" / ProjectId(projectId) / "webhooks" as authUser                => deleteHook(projectId, authUser)
      case POST   -> Root / "projects" / ProjectId(projectId) / "webhooks" / "validation" as authUser => validateHook(projectId, authUser)
      case _ => NotFound()
    }
  }

  private lazy val optionalAuthorizedRoutes: HttpRoutes[F] = {
    val x = AuthedRoutes.of[Option[AuthUser], F] {
      case GET -> Root / "projects" / ProjectId(projectId) / "events" / "status" as authUser =>
        fetchProcessingStatus(projectId, authUser)
    }

    optAuthMiddleware(x)
  }

  private lazy val nonAuthorizedRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case           GET  -> Root / "ping"                => Ok("pong")
    case request @ POST -> Root / "webhooks" / "events" => processPushEvent(request)
  }
  // format: on

  lazy val routes: Resource[F, HttpRoutes[F]] =
    (versionRoutes() <+> nonAuthorizedRoutes <+> optionalAuthorizedRoutes <+> authorizedRoutes).withMetrics
}

private object MicroserviceRoutes {
  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry: ExecutionTimeRecorder]: F[MicroserviceRoutes[F]] = for {
    projectHookUrl         <- ProjectHookUrl.fromConfig[F]()
    hookTokenCrypto        <- HookTokenCrypto[F]()
    webhookEventsEndpoint  <- webhookevents.Endpoint(hookTokenCrypto)
    hookCreatorEndpoint    <- HookCreationEndpoint(projectHookUrl, hookTokenCrypto)
    eventStatusEndpoint    <- eventstatus.Endpoint(projectHookUrl)
    hookValidationEndpoint <- HookValidationEndpoint(projectHookUrl)
    hookDeletionEndpoint   <- HookDeletionEndpoint(projectHookUrl)
    authenticator          <- GitLabAuthenticator[F]
    authMiddleware         <- Authentication.middleware(authenticator)
    optAuthMiddleware      <- Authentication.middlewareAuthenticatingIfNeeded(authenticator)
    versionRoutes          <- version.Routes[F]
  } yield new MicroserviceRoutes[F](
    webhookEventsEndpoint,
    hookCreatorEndpoint,
    hookValidationEndpoint,
    hookDeletionEndpoint,
    eventStatusEndpoint,
    authMiddleware,
    optAuthMiddleware,
    new RoutesMetrics[F],
    versionRoutes
  )
}
