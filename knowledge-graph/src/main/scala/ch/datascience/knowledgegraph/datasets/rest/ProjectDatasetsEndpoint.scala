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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect._
import cats.syntax.all._
import ch.datascience.config.renku
import ch.datascience.http.InfoMessage._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import ch.datascience.graph.model.projects
import ch.datascience.http.ErrorMessage
import ch.datascience.http.rest.Links._
import ch.datascience.knowledgegraph.datasets.rest.ProjectDatasetsFinder.ProjectDataset
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import io.chrisdavenport.log4cats.Logger
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class ProjectDatasetsEndpoint[Interpretation[_]: Effect](
    projectDatasetsFinder: ProjectDatasetsFinder[Interpretation],
    renkuResourcesUrl:     renku.ResourcesUrl,
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  import ProjectDatasetsFinder.SameAsOrDerived
  import executionTimeRecorder._
  import org.http4s.circe._

  def getProjectDatasets(projectPath: projects.Path): Interpretation[Response[Interpretation]] =
    measureExecutionTime {
      projectDatasetsFinder
        .findProjectDatasets(projectPath)
        .flatMap(datasets => Ok(datasets.asJson))
        .recoverWith(httpResult(projectPath))
    } map logExecutionTimeWhen(finishedSuccessfully(projectPath))

  private def httpResult(
      projectPath: projects.Path
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Finding $projectPath's datasets failed")
    logger.error(exception)(errorMessage.value)
    InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(projectPath: projects.Path): PartialFunction[Response[Interpretation], String] = {
    case response if response.status == Ok => s"Finding '$projectPath' datasets finished"
  }

  private implicit val sameAsOrDerivedEncoder: Encoder[SameAsOrDerived] = Encoder.instance[SameAsOrDerived] {
    case Left(sameAs: SameAs) => json"""{"sameAs": ${sameAs.toString}}"""
    case Right(derivedFrom: DerivedFrom) => json"""{"derivedFrom": ${derivedFrom.toString}}"""
  }

  private implicit val datasetEncoder: Encoder[ProjectDataset] =
    Encoder.instance[ProjectDataset] { case (id, initialVersion, title, name, sameAsOrDerived, images) =>
      json"""{
          "identifier": ${id.toString},
          "versions": {
            "initial": ${initialVersion.toString}
          },
          "title": ${title.toString},
          "name": ${name.toString},
          "images": ${images.map(_.value)}
        }"""
        .deepMerge(sameAsOrDerived.asJson)
        .deepMerge(
          _links(
            Rel("details")         -> Href(renkuResourcesUrl / "datasets" / id),
            Rel("initial-version") -> Href(renkuResourcesUrl / "datasets" / initialVersion)
          )
        )
    }
}

object IOProjectDatasetsEndpoint {

  def apply(
      timeRecorder: SparqlQueryTimeRecorder[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ProjectDatasetsEndpoint[IO]] =
    for {
      rdfStoreConfig        <- RdfStoreConfig[IO]()
      renkuBaseUrl          <- RenkuBaseUrl[IO]()
      renkuResourceUrl      <- renku.ResourcesUrl[IO]()
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
    } yield new ProjectDatasetsEndpoint[IO](
      new IOProjectDatasetsFinder(rdfStoreConfig, renkuBaseUrl, ApplicationLogger, timeRecorder),
      renkuResourceUrl,
      executionTimeRecorder,
      ApplicationLogger
    )
}
