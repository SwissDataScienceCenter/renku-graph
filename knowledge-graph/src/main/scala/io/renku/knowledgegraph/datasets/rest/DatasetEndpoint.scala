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

package io.renku.knowledgegraph.datasets.rest

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.datasets.{Date, DateCreated, DatePublished, Identifier, ImageUri}
import io.renku.http.InfoMessage._
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.knowledgegraph.datasets.model._
import io.renku.knowledgegraph.projects.rest.ProjectEndpoint
import io.renku.logging.ExecutionTimeRecorder
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait DatasetEndpoint[F[_]] {
  def getDataset(identifier: Identifier, authContext: AuthContext[Identifier]): F[Response[F]]
}

class DatasetEndpointImpl[F[_]: MonadThrow: Logger](
    datasetFinder:         DatasetFinder[F],
    renkuResourcesUrl:     renku.ResourcesUrl,
    gitLabUrl:             GitLabUrl,
    executionTimeRecorder: ExecutionTimeRecorder[F]
) extends Http4sDsl[F]
    with DatasetEndpoint[F] {

  import executionTimeRecorder._
  import io.renku.tinytypes.json.TinyTypeEncoders._
  import org.http4s.circe._

  def getDataset(identifier: Identifier, authContext: AuthContext[Identifier]): F[Response[F]] = measureExecutionTime {
    datasetFinder
      .findDataset(identifier, authContext)
      .flatMap(toHttpResult(identifier))
      .recoverWith(httpResult(identifier))
  } map logExecutionTimeWhen(finishedSuccessfully(identifier))

  private def toHttpResult(
      identifier: Identifier
  ): Option[Dataset] => F[Response[F]] = {
    case None          => NotFound(InfoMessage(s"No dataset with '$identifier' id found"))
    case Some(dataset) => Ok(dataset.asJson)
  }

  private def httpResult(
      identifier: Identifier
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Finding dataset with '$identifier' id failed")
    Logger[F].error(exception)(errorMessage.value) >>
      InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(identifier: Identifier): PartialFunction[Response[F], String] = {
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
        ("url" -> dataset.resourceId.asJson).some,
        dataset match {
          case ds: NonModifiedDataset => ("sameAs" -> ds.sameAs.asJson).some
          case ds: ModifiedDataset    => ("derivedFrom" -> ds.derivedFrom.asJson).some
        },
        ("versions" -> dataset.versions.asJson).some,
        dataset.maybeDescription.map(description => "description" -> description.asJson),
        ("published" -> (dataset.creators -> dataset.date).asJson).some,
        dataset.date match {
          case DatePublished(_)  => Option.empty[(String, Json)]
          case DateCreated(date) => ("created" -> date.asJson).some
        },
        ("hasPart" -> dataset.parts.asJson).some,
        ("project" -> dataset.project.asJson).some,
        ("usedIn" -> dataset.usedIn.asJson).some,
        ("keywords" -> dataset.keywords.asJson).some,
        ("images" -> (dataset.images, dataset.project).asJson).some
      ).flatten: _*
    ) deepMerge _links(
      Rel.Self -> DatasetEndpoint.href(renkuResourcesUrl, dataset.id),
      Rel("initial-version") -> DatasetEndpoint.href(renkuResourcesUrl, dataset.versions.initial.value)
    )
  }
  // format: on

  private implicit lazy val publishingEncoder: Encoder[(Set[DatasetCreator], Date)] =
    Encoder.instance {
      case (creators, DatePublished(date)) =>
        Json.obj(
          "datePublished" -> date.asJson,
          "creator"       -> creators.toList.asJson
        )
      case (creators, _) =>
        Json.obj(
          "creator" -> creators.toList.asJson
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
      "atLocation": ${part.location}
    }"""
  }

  private implicit lazy val projectEncoder: Encoder[DatasetProject] = Encoder.instance[DatasetProject] { project =>
    json"""{
      "path": ${project.path},
      "name": ${project.name}
    }""" deepMerge _links(Link(Rel("project-details") -> ProjectEndpoint.href(renkuResourcesUrl, project.path)))
  }

  private implicit lazy val versionsEncoder: Encoder[DatasetVersions] = Encoder.instance[DatasetVersions] { versions =>
    json"""{
      "initial": ${versions.initial}
    }"""
  }

  private implicit lazy val imagesEncoder: Encoder[(List[ImageUri], DatasetProject)] =
    Encoder.instance[(List[ImageUri], DatasetProject)] { case (imageUris, project) =>
      Json.arr(imageUris.map {
        case uri: ImageUri.Relative =>
          json"""{
            "location": $uri
          }""" deepMerge _links(
            Link(Rel("view") -> Href(gitLabUrl / project.path / "raw" / "master" / uri))
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

object DatasetEndpoint {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DatasetEndpoint[F]] = for {
    datasetFinder         <- DatasetFinder[F]
    renkuResourceUrl      <- renku.ResourcesUrl[F]()
    gitLabUrl             <- GitLabUrlLoader[F]()
    executionTimeRecorder <- ExecutionTimeRecorder[F]()
  } yield new DatasetEndpointImpl[F](datasetFinder, renkuResourceUrl, gitLabUrl, executionTimeRecorder)

  def href(renkuResourcesUrl: renku.ResourcesUrl, identifier: Identifier): Href =
    Href(renkuResourcesUrl / "datasets" / identifier)
}
