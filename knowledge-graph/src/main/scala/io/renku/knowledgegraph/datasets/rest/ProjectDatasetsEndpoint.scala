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

package io.renku.knowledgegraph.datasets.rest

import ProjectDatasetsFinder.ProjectDataset
import cats.effect._
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.graph.config.{GitLabUrlLoader, RenkuBaseUrlLoader}
import io.renku.graph.model.datasets.{DerivedFrom, ImageUri, SameAs}
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.rest.Links._
import io.renku.logging.{ApplicationLogger, ExecutionTimeRecorder}
import io.renku.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import io.renku.tinytypes.json.TinyTypeEncoders
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class ProjectDatasetsEndpoint[Interpretation[_]: Effect](
    projectDatasetsFinder: ProjectDatasetsFinder[Interpretation],
    renkuResourcesUrl:     renku.ResourcesUrl,
    gitLabUrl:             GitLabUrl,
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
) extends Http4sDsl[Interpretation]
    with TinyTypeEncoders {

  import ProjectDatasetsFinder.SameAsOrDerived
  import executionTimeRecorder._
  import org.http4s.circe._

  def getProjectDatasets(projectPath: projects.Path): Interpretation[Response[Interpretation]] = measureExecutionTime {
    implicit val encoder: Encoder[ProjectDataset] = datasetEncoder(projectPath)

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

  private def datasetEncoder(projectPath: projects.Path): Encoder[ProjectDataset] =
    Encoder.instance[ProjectDataset] { case (id, initialVersion, title, name, sameAsOrDerived, images) =>
      json"""{
          "identifier": ${id.toString},
          "versions": {
            "initial": ${initialVersion.toString}
          },
          "title": ${title.toString},
          "name": ${name.toString},
          "images": ${images -> projectPath}
        }"""
        .deepMerge(sameAsOrDerived.asJson)
        .deepMerge(
          _links(
            Rel("details")         -> Href(renkuResourcesUrl / "datasets" / id),
            Rel("initial-version") -> Href(renkuResourcesUrl / "datasets" / initialVersion)
          )
        )
    }

  private implicit lazy val imagesEncoder: Encoder[(List[ImageUri], projects.Path)] =
    Encoder.instance[(List[ImageUri], projects.Path)] { case (imageUris, exemplarProjectPath) =>
      Json.arr(imageUris.map {
        case uri: ImageUri.Relative =>
          json"""{
            "location": $uri  
          }""" deepMerge _links(
            Link(Rel("view") -> Href(gitLabUrl / exemplarProjectPath / "raw" / "master" / uri))
          )
        case uri: ImageUri.Absolute =>
          json"""{
            "location": $uri  
          }""" deepMerge _links(
            Link(Rel("view") -> Href(uri.show))
          )
      }: _*)
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
      renkuBaseUrl          <- RenkuBaseUrlLoader[IO]()
      gitLabUrl             <- GitLabUrlLoader[IO]()
      renkuResourceUrl      <- renku.ResourcesUrl[IO]()
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
      projectDatasetFinder  <- ProjectDatasetsFinder(rdfStoreConfig, renkuBaseUrl, ApplicationLogger, timeRecorder)
    } yield new ProjectDatasetsEndpoint[IO](
      projectDatasetFinder,
      renkuResourceUrl,
      gitLabUrl,
      executionTimeRecorder,
      ApplicationLogger
    )
}
