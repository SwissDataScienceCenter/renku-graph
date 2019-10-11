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

package ch.datascience.knowledgegraph

import cats.effect.ConcurrentEffect
import ch.datascience.graph.http.server.binders.ProjectPath._
import ch.datascience.knowledgegraph.datasets.rest.{DatasetId, DatasetsEndpoint, ProjectDatasetsEndpoint}
import ch.datascience.knowledgegraph.graphql.QueryEndpoint
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    queryEndpoint:           QueryEndpoint[F],
    projectDatasetsEndpoint: ProjectDatasetsEndpoint[F],
    datasetsEndpoint:        DatasetsEndpoint[F]
) extends Http4sDsl[F] {

  import datasetsEndpoint._
  import org.http4s.HttpRoutes
  import projectDatasetsEndpoint._
  import queryEndpoint._

  // format: off
  lazy val routes: HttpRoutes[F] = HttpRoutes.of[F] {
    case           GET  -> Root / "ping"                                                                          => Ok("pong")
    case           GET  -> Root / "knowledge-graph" / "datasets" / DatasetId(id)                                  => getDataset(id)
    case           GET  -> Root / "knowledge-graph" / "graphql"                                                   => schema
    case request @ POST -> Root / "knowledge-graph" / "graphql"                                                   => handleQuery(request)
    case           GET  -> Root / "knowledge-graph" / "projects" / Namespace(namespace) / Name(name) / "datasets" => getProjectDatasets(namespace / name)
  }
  // format: on
}
