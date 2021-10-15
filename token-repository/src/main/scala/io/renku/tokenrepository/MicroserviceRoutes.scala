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

package io.renku.tokenrepository

import cats.effect.{Clock, ConcurrentEffect, Resource}
import ch.datascience.graph.http.server.binders.{ProjectId, ProjectPath}
import ch.datascience.metrics.RoutesMetrics
import io.renku.tokenrepository.repository.association.AssociateTokenEndpoint
import io.renku.tokenrepository.repository.deletion.DeleteTokenEndpoint
import io.renku.tokenrepository.repository.fetching.FetchTokenEndpoint
import org.http4s.dsl.Http4sDsl

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    fetchTokenEndpoint:     FetchTokenEndpoint[F],
    associateTokenEndpoint: AssociateTokenEndpoint[F],
    deleteTokenEndpoint:    DeleteTokenEndpoint[F],
    routesMetrics:          RoutesMetrics[F]
)(implicit clock:           Clock[F])
    extends Http4sDsl[F] {

  import associateTokenEndpoint._
  import deleteTokenEndpoint._
  import fetchTokenEndpoint._
  import org.http4s.HttpRoutes
  import routesMetrics._

  // format: off
  lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case           GET    -> Root / "ping"                                           => Ok("pong")
    case           GET    -> Root / "projects" / ProjectId(projectId) / "tokens"     => fetchToken(projectId)
    case           GET    -> Root / "projects" / ProjectPath(projectPath) / "tokens" => fetchToken(projectPath)
    case request @ PUT    -> Root / "projects" / ProjectId(projectId) / "tokens"     => associateToken(projectId, request)
    case           DELETE -> Root / "projects" / ProjectId(projectId) / "tokens"     => deleteToken(projectId)
  }.withMetrics
  // format: on
}
