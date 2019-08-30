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

package ch.datascience.knowledgegraph.datasets.rest

import cats.implicits._
import ch.datascience.config.{RenkuBaseUrl, RenkuResourcesUrl}
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.model.dataSets.{Identifier, Name}
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.rest.Links
import cats.effect.{ContextShift, Effect, IO, Timer}
import Links._
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.RdfStoreConfig
import io.chrisdavenport.log4cats.Logger
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class ProjectDataSetsEndpoint[Interpretation[_]: Effect](
    projectDataSetsFinder: ProjectDataSetsFinder[Interpretation],
    renkuResourcesUrl:     RenkuResourcesUrl,
    logger:                Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  import org.http4s.circe._

  def getProjectDataSets(projectPath: ProjectPath): Interpretation[Response[Interpretation]] =
    projectDataSetsFinder
      .findProjectDataSets(projectPath)
      .flatMap(toHttpResult(projectPath))
      .recoverWith(httpResult(projectPath))

  private def toHttpResult(
      projectPath: ProjectPath
  ): List[(Identifier, Name)] => Interpretation[Response[Interpretation]] = {
    case Nil           => NotFound(InfoMessage(s"No data-sets found for '$projectPath'"))
    case dataSetsInfos => Ok(dataSetsInfos.asJson)
  }

  private def httpResult(
      projectPath: ProjectPath
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Finding $projectPath's data-sets failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private implicit val dataSetEncoder: Encoder[(Identifier, Name)] = Encoder.instance[(Identifier, Name)] {
    case (id, name) =>
      json"""
      {
        "identifier": ${id.toString},
        "name": ${name.toString}
      }""" deepMerge _links(
        Link(Rel("details") -> Href(renkuResourcesUrl / "data-sets" / id))
      )
  }
}

object IOProjectDataSetsEndpoint {

  def apply()(implicit executionContext: ExecutionContext,
              contextShift:              ContextShift[IO],
              timer:                     Timer[IO]): IO[ProjectDataSetsEndpoint[IO]] =
    for {
      rdfStoreConfig   <- RdfStoreConfig[IO]()
      renkuBaseUrl     <- RenkuBaseUrl[IO]()
      renkuResourceUrl <- RenkuResourcesUrl[IO]()
    } yield
      new ProjectDataSetsEndpoint[IO](
        new IOProjectDataSetsFinder(rdfStoreConfig, renkuBaseUrl, ApplicationLogger),
        renkuResourceUrl,
        ApplicationLogger
      )
}
