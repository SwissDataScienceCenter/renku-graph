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

import cats.effect._
import cats.implicits._
import ch.datascience.config.RenkuResourcesUrl
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.model.datasets.Identifier
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.ApplicationLogger
import io.chrisdavenport.log4cats.Logger
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class DatasetsEndpoint[Interpretation[_]: Effect](
    datasetFinder:     DatasetFinder[Interpretation],
    renkuResourcesUrl: RenkuResourcesUrl,
    logger:            Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  import ch.datascience.tinytypes.json.TinyTypeEncoders._
  import org.http4s.circe._

  def getDataset(identifier: Identifier): Interpretation[Response[Interpretation]] =
    datasetFinder
      .findDataset(identifier)
      .flatMap(toHttpResult(identifier))
      .recoverWith(httpResult(identifier))

  private def toHttpResult(
      identifier: Identifier
  ): Option[Dataset] => Interpretation[Response[Interpretation]] = {
    case None          => NotFound(InfoMessage(s"No dataset with '$identifier' id found"))
    case Some(dataset) => Ok(dataset.asJson)
  }

  private def httpResult(
      identifier: Identifier
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Finding dataset with '$identifier' id failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  // format: off
  private implicit lazy val datasetEncoder: Encoder[Dataset] = Encoder.instance[Dataset] { dataset =>
    Json.obj(
      List(
        Some("identifier" -> dataset.id.asJson),
        Some("name" -> dataset.name.asJson),
        dataset.maybeDescription.map(description => "description" -> description.asJson),
        Some("created" ->
          json"""{
          "dateCreated": ${dataset.created.date},
          "agent": {
            "email": ${dataset.created.agent.email},
            "name": ${dataset.created.agent.name}
          }
        }"""),
        Some("published" -> Json.obj(List(
          dataset.published.maybeDate.map(date => "datePublished" -> date.asJson),
          Some("creator" -> dataset.published.creators.toList.asJson)
        ).flatten: _*)),
        Some("hasPart" -> dataset.part.asJson),
        Some("isPartOf" -> dataset.project.asJson)
      ).flatten: _*
    ) deepMerge _links(
      Link(Rel.Self -> Href(renkuResourcesUrl / "datasets" / dataset.id))
    )
  } 
  // format: on

  // format: off
  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] { creator =>
    Json.obj(List(
      Some("name" -> creator.name.asJson),
      creator.maybeEmail.map(email => "email" -> email.asJson)
    ).flatten: _*)
  }
  // format: on

  private implicit lazy val partEncoder: Encoder[DatasetPart] = Encoder.instance[DatasetPart] { part =>
    json"""{
      "name": ${part.name},
      "atLocation": ${part.atLocation},
      "dateCreated": ${part.dateCreated}
    }"""
  }

  private implicit lazy val projectEncoder: Encoder[DatasetProject] = Encoder.instance[DatasetProject] { project =>
    json"""{
      "name": ${project.name}
    }"""
  }
}

object IODatasetsEndpoint {

  def apply()(implicit executionContext: ExecutionContext,
              contextShift:              ContextShift[IO],
              timer:                     Timer[IO]): IO[DatasetsEndpoint[IO]] =
    for {
      datasetFinder    <- IODatasetFinder(logger = ApplicationLogger)
      renkuResourceUrl <- RenkuResourcesUrl[IO]()
    } yield
      new DatasetsEndpoint[IO](
        datasetFinder,
        renkuResourceUrl,
        ApplicationLogger
      )
}
