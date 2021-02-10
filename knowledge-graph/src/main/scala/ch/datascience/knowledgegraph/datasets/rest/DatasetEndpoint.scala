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
import ch.datascience.graph.model.datasets.Identifier
import ch.datascience.http.InfoMessage._
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import io.chrisdavenport.log4cats.Logger
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class DatasetEndpoint[Interpretation[_]: Effect](
    datasetFinder:         DatasetFinder[Interpretation],
    renkuResourcesUrl:     renku.ResourcesUrl,
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  import ch.datascience.tinytypes.json.TinyTypeEncoders._
  import executionTimeRecorder._
  import org.http4s.circe._

  def getDataset(identifier: Identifier): Interpretation[Response[Interpretation]] =
    measureExecutionTime {
      datasetFinder
        .findDataset(identifier)
        .flatMap(toHttpResult(identifier))
        .recoverWith(httpResult(identifier))
    } map logExecutionTimeWhen(finishedSuccessfully(identifier))

  private def toHttpResult(
      identifier: Identifier
  ): Option[Dataset] => Interpretation[Response[Interpretation]] = {
    case None          => NotFound(InfoMessage(s"No dataset with '$identifier' id found"))
    case Some(dataset) => Ok(dataset.asJson)
  }

  private def httpResult(
      identifier: Identifier
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Finding dataset with '$identifier' id failed")
    logger.error(exception)(errorMessage.value)
    InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(identifier: Identifier): PartialFunction[Response[Interpretation], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      s"Finding '$identifier' dataset finished"
  }

  // format: off
  private implicit lazy val datasetEncoder: Encoder[Dataset] = Encoder.instance[Dataset] { dataset =>
    Json.obj(
      List(
        ("identifier" -> dataset.id.asJson).some,
        ("name" -> dataset.name.asJson).some,
        ("title" -> dataset.title.asJson).some,
        ("url" -> dataset.url.asJson).some,
        dataset match {
          case ds: NonModifiedDataset => ("sameAs" -> ds.sameAs.asJson).some
          case ds: ModifiedDataset    => ("derivedFrom" -> ds.derivedFrom.asJson).some
        },
        ("versions" -> dataset.versions.asJson).some,
        dataset.maybeDescription.map(description => "description" -> description.asJson),
        ("published" -> dataset.published.asJson).some,
        ("created" -> dataset.created.asJson).some,
        ("hasPart" -> dataset.parts.asJson).some,
        ("isPartOf" -> dataset.projects.asJson).some,
        ("keywords" -> dataset.keywords.asJson).some,
        ("images" -> dataset.images.asJson).some
      ).flatten: _*
    ) deepMerge _links(
      Rel.Self -> Href(renkuResourcesUrl / "datasets" / dataset.id),
      Rel("initial-version") -> Href(renkuResourcesUrl / "datasets" / dataset.versions.initial)
    )
  }
  // format: on

  private implicit lazy val publishingEncoder: Encoder[DatasetPublishing] = Encoder.instance[DatasetPublishing] {
    published =>
      Json.obj(
        List(
          published.maybeDate.map(date => "datePublished" -> date.asJson),
          ("creator" -> published.creators.toList.asJson).some
        ).flatten: _*
      )
  }

  // format: off
  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] { creator =>
    Json.obj(List(
      ("name" -> creator.name.asJson).some,
      creator.maybeEmail.map(email => "email" -> email.asJson),
      creator.maybeAffiliation.map(affiliation => "affiliation" -> affiliation.asJson)
    ).flatten: _*)
  }
  // format: on

  private implicit lazy val partEncoder: Encoder[DatasetPart] = Encoder.instance[DatasetPart] { part =>
    json"""{
      "name": ${part.name},
      "atLocation": ${part.atLocation}
    }"""
  }

  private implicit lazy val projectEncoder: Encoder[DatasetProject] = Encoder.instance[DatasetProject] { project =>
    json"""{
      "path": ${project.path},
      "name": ${project.name},
      "created": {
        "dateCreated": ${project.created.date},
        "agent": {
          "email": ${project.created.agent.maybeEmail},
          "name": ${project.created.agent.name}
        }
      }
    }""" deepMerge _links(
      Link(Rel("project-details") -> Href(renkuResourcesUrl / "projects" / project.path))
    )
  }

  private implicit lazy val versionsEncoder: Encoder[DatasetVersions] = Encoder.instance[DatasetVersions] { versions =>
    json"""{
      "initial": ${versions.initial}
    }"""
  }
}

object IODatasetEndpoint {

  def apply(
      timeRecorder: SparqlQueryTimeRecorder[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[DatasetEndpoint[IO]] =
    for {
      datasetFinder         <- IODatasetFinder(timeRecorder, logger = ApplicationLogger)
      renkuResourceUrl      <- renku.ResourcesUrl[IO]()
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
    } yield new DatasetEndpoint[IO](
      datasetFinder,
      renkuResourceUrl,
      executionTimeRecorder,
      ApplicationLogger
    )
}
