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

package io.renku.knowledgegraph.datasets

import ProjectDatasetsFinder.ProjectDataset
import cats.effect._
import cats.syntax.all._
import io.circe.Encoder
import io.circe.syntax._
import io.renku.config.renku
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.rest.Links._
import io.renku.logging.ExecutionTimeRecorder
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesstore.{RenkuConnectionConfig, SparqlQueryTimeRecorder}
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait ProjectDatasetsEndpoint[F[_]] {
  def getProjectDatasets(projectPath: projects.Path): F[Response[F]]
}

class ProjectDatasetsEndpointImpl[F[_]: MonadCancelThrow: Logger](
    projectDatasetsFinder: ProjectDatasetsFinder[F],
    renkuApiUrl:           renku.ApiUrl,
    gitLabUrl:             GitLabUrl,
    executionTimeRecorder: ExecutionTimeRecorder[F]
) extends Http4sDsl[F]
    with TinyTypeEncoders
    with ProjectDatasetsEndpoint[F] {

  import executionTimeRecorder._
  import org.http4s.circe._

  private implicit lazy val apiUrl: renku.ApiUrl = renkuApiUrl
  private implicit lazy val glUrl:  GitLabUrl    = gitLabUrl

  def getProjectDatasets(projectPath: projects.Path): F[Response[F]] = measureExecutionTime {
    implicit val encoder: Encoder[ProjectDataset] = ProjectDatasetEncoder.encoder(projectPath)

    projectDatasetsFinder
      .findProjectDatasets(projectPath)
      .flatMap(datasets => Ok(datasets.asJson))
      .recoverWith(httpResult(projectPath))
  } map logExecutionTimeWhen(finishedSuccessfully(projectPath))

  private def httpResult(
      projectPath: projects.Path
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Finding $projectPath's datasets failed")
    Logger[F].error(exception)(errorMessage.value) >>
      InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(projectPath: projects.Path): PartialFunction[Response[F], String] = {
    case response if response.status == Ok => s"Finding '$projectPath' datasets finished"
  }
}

object ProjectDatasetsEndpoint {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectDatasetsEndpoint[F]] = for {
    renkuConnectionConfig <- RenkuConnectionConfig[F]()
    gitLabUrl             <- GitLabUrlLoader[F]()
    renkuResourceUrl      <- renku.ApiUrl[F]()
    executionTimeRecorder <- ExecutionTimeRecorder[F]()
    projectDatasetFinder  <- ProjectDatasetsFinder(renkuConnectionConfig)
  } yield new ProjectDatasetsEndpointImpl[F](
    projectDatasetFinder,
    renkuResourceUrl,
    gitLabUrl,
    executionTimeRecorder
  )

  def href(renkuApiUrl: renku.ApiUrl, projectPath: projects.Path): Href =
    Href(renkuApiUrl / "projects" / projectPath / "datasets")
}
