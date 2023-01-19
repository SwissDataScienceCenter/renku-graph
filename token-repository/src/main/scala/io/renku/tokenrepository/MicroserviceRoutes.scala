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

package io.renku.tokenrepository

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.renku.graph.http.server.binders.{ProjectId, ProjectPath}
import io.renku.http.client.GitLabClient
import io.renku.http.server.version
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.creation.CreateTokenEndpoint
import io.renku.tokenrepository.repository.deletion.DeleteTokenEndpoint
import io.renku.tokenrepository.repository.fetching.FetchTokenEndpoint
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}
import org.typelevel.log4cats.Logger

private trait MicroserviceRoutes[F[_]] {
  def notifyDBReady(): F[Unit]
  def routes:          Resource[F, HttpRoutes[F]]
}

private class MicroserviceRoutesImpl[F[_]: MonadThrow](
    fetchTokenEndpoint:     FetchTokenEndpoint[F],
    associateTokenEndpoint: CreateTokenEndpoint[F],
    deleteTokenEndpoint:    DeleteTokenEndpoint[F],
    routesMetrics:          RoutesMetrics[F],
    versionRoutes:          version.Routes[F],
    dbReady:                Ref[F, Boolean]
)(implicit clock: Clock[F])
    extends Http4sDsl[F]
    with MicroserviceRoutes[F] {

  import associateTokenEndpoint._
  import deleteTokenEndpoint._
  import fetchTokenEndpoint._
  import io.renku.http.InfoMessage
  import io.renku.http.InfoMessage._
  import org.http4s.HttpRoutes
  import routesMetrics._

  override def notifyDBReady(): F[Unit] = dbReady.set(true)

  // format: off
  override lazy val routes: Resource[F, HttpRoutes[F]] = HttpRoutes.of[F] {
    case       GET    -> Root / "ping"                                           => Ok("pong")
    case       GET    -> Root / "projects" / ProjectId(projectId) / "tokens"     => whenDBReady(fetchToken(projectId))
    case       GET    -> Root / "projects" / ProjectPath(projectPath) / "tokens" => whenDBReady(fetchToken(projectPath))
    case req @ POST   -> Root / "projects" / ProjectId(projectId) / "tokens"     => whenDBReady(createToken(projectId, req))
    case       DELETE -> Root / "projects" / ProjectId(projectId) / "tokens"     => whenDBReady(deleteToken(projectId))
  }.withMetrics.map(_ <+> versionRoutes())
  // format: on

  private def whenDBReady(thunk: => F[Response[F]]): F[Response[F]] = dbReady.get >>= {
    case true  => thunk
    case false => ServiceUnavailable(InfoMessage("DB migration running"))
  }
}

private object MicroserviceRoutes {
  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry: SessionResource: QueriesExecutionTimes]
      : F[MicroserviceRoutes[F]] = for {
    fetchTokenEndpoint  <- FetchTokenEndpoint[F]
    createTokenEndpoint <- CreateTokenEndpoint[F]
    deleteTokenEndpoint <- DeleteTokenEndpoint[F]
    versionRoutes       <- version.Routes[F]
    dbReady             <- Ref.of(false)
  } yield new MicroserviceRoutesImpl(fetchTokenEndpoint,
                                     createTokenEndpoint,
                                     deleteTokenEndpoint,
                                     new RoutesMetrics[F],
                                     versionRoutes,
                                     dbReady
  )
}
