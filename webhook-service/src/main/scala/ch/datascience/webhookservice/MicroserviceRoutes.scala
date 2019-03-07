/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice

import cats.effect.ConcurrentEffect
import ch.datascience.graph.http.server.ProjectIdPathBinder
import ch.datascience.webhookservice.eventprocessing.HookEventEndpoint
import ch.datascience.webhookservice.hookcreation.HookCreationEndpoint
import ch.datascience.webhookservice.hookvalidation.HookValidationEndpoint
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    hookEventEndpoint:      HookEventEndpoint[F],
    hookCreationEndpoint:   HookCreationEndpoint[F],
    hookValidationEndpoint: HookValidationEndpoint[F]
) extends Http4sDsl[F] {

  import org.http4s.HttpRoutes

  lazy val routes: HttpRoutes[F] = HttpRoutes
    .of[F] {
      case GET -> Root / "ping" => Ok("pong")
      case request @ POST -> Root / "webhooks" / "events" =>
        hookEventEndpoint.processPushEvent(request)
      case request @ POST -> Root / "projects" / ProjectIdPathBinder(projectId) / "webhooks" =>
        hookCreationEndpoint.createHook(projectId, request)
      case request @ POST -> Root / "projects" / ProjectIdPathBinder(projectId) / "webhooks" / "validation" =>
        hookValidationEndpoint.validateHook(projectId, request)
    }
}
