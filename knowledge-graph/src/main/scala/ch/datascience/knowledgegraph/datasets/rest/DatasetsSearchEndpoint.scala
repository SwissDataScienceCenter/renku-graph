/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.MonadError
import cats.effect._
import cats.implicits._
import ch.datascience.config._
import ch.datascience.config.renku.ResourceUrl
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.InfoMessage._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.knowledgegraph.datasets.model.{DatasetCreator, DatasetPublishing}
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.chrisdavenport.log4cats.Logger
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.impl.OptionalValidatingQueryParamDecoderMatcher
import org.http4s.{ParseFailure, QueryParamDecoder, QueryParameterValue, Response}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class DatasetsSearchEndpoint[Interpretation[_]: Effect](
    datasetsFinder:        DatasetsFinder[Interpretation],
    renkuResourcesUrl:     renku.ResourcesUrl,
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import DatasetsFinder.DatasetSearchResult
  import DatasetsSearchEndpoint.Query._
  import DatasetsSearchEndpoint.Sort
  import PagingRequest.Decoders._
  import ch.datascience.json.JsonOps._
  import ch.datascience.tinytypes.json.TinyTypeEncoders._
  import executionTimeRecorder._
  import io.circe.Encoder
  import io.circe.literal._

  def searchForDatasets(maybePhrase: Option[Phrase],
                        sort:        Sort.By,
                        paging:      PagingRequest): Interpretation[Response[Interpretation]] =
    measureExecutionTime {
      implicit val datasetsUrl: renku.ResourceUrl = requestedUrl(maybePhrase, sort, paging)

      datasetsFinder
        .findDatasets(maybePhrase, sort, paging)
        .map(_.toHttpResponse)
        .recoverWith(httpResult(maybePhrase))
    } map logExecutionTimeWhen(finishedSuccessfully(maybePhrase))

  private def requestedUrl(maybePhrase: Option[Phrase], sort: Sort.By, paging: PagingRequest): renku.ResourceUrl =
    (renkuResourcesUrl / "datasets") ? (page.parameterName -> paging.page) & (perPage.parameterName -> paging.perPage) & (Sort.sort.parameterName -> sort) && (query.parameterName -> maybePhrase)

  private def httpResult(
      maybePhrase: Option[Phrase]
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(
        maybePhrase
          .map(phrase => s"Finding datasets matching '$phrase' failed")
          .getOrElse("Finding all datasets failed")
      )
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(maybePhrase: Option[Phrase]): PartialFunction[Response[Interpretation], String] = {
    case response if response.status == Ok =>
      maybePhrase
        .map(phrase => s"Finding datasets containing '$phrase' phrase finished")
        .getOrElse("Finding all datasets finished")
  }

  private implicit val datasetEncoder: Encoder[DatasetSearchResult] = Encoder.instance[DatasetSearchResult] {
    case DatasetSearchResult(id, name, maybeDescription, published, projectsCount) =>
      json"""
      {
        "identifier": $id,
        "name": $name,
        "published": $published,
        "projectsCount": $projectsCount
      }"""
        .addIfDefined("description"           -> maybeDescription)
        .deepMerge(_links(Link(Rel("details") -> Href(renkuResourcesUrl / "datasets" / id))))
  }

  private implicit lazy val publishingEncoder: Encoder[DatasetPublishing] = Encoder.instance[DatasetPublishing] {
    case DatasetPublishing(maybeDate, creators) =>
      json"""{
        "creator": $creators
      }""" addIfDefined "datePublished" -> maybeDate
  }

  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] {
    case DatasetCreator(maybeEmail, name, _) =>
      json"""{
        "name": $name
      }""" addIfDefined ("email" -> maybeEmail)
  }
}

object DatasetsSearchEndpoint {

  object Query {
    final class Phrase private (val value: String) extends AnyVal with StringTinyType
    implicit object Phrase extends TinyTypeFactory[Phrase](new Phrase(_)) with NonBlank

    private implicit val queryParameterDecoder: QueryParamDecoder[Phrase] =
      (value: QueryParameterValue) =>
        Phrase
          .from(value.value)
          .leftMap(_ => ParseFailure(s"'${query.parameterName}' parameter with invalid value", ""))
          .toValidatedNel

    object query extends OptionalValidatingQueryParamDecoderMatcher[Phrase]("query") {
      val parameterName: String = "query"
    }
  }

  object Sort extends ch.datascience.http.rest.SortBy {

    type PropertyType = SearchProperty

    sealed trait SearchProperty             extends Property
    final case object NameProperty          extends Property("name") with SearchProperty
    final case object DatePublishedProperty extends Property("datePublished") with SearchProperty
    final case object ProjectsCountProperty extends Property("projectsCount") with SearchProperty

    override lazy val properties: Set[SearchProperty] = Set(NameProperty, DatePublishedProperty, ProjectsCountProperty)
  }
}

object IODatasetsSearchEndpoint {

  def apply(
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[DatasetsSearchEndpoint[IO]] =
    for {
      rdfStoreConfig        <- RdfStoreConfig[IO]()
      renkuBaseUrl          <- RenkuBaseUrl[IO]()
      renkuResourceUrl      <- renku.ResourcesUrl[IO]()
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
    } yield new DatasetsSearchEndpoint[IO](
      new IODatasetsFinder(rdfStoreConfig,
                           new CreatorsFinder(rdfStoreConfig, renkuBaseUrl, ApplicationLogger, timeRecorder),
                           ApplicationLogger,
                           timeRecorder),
      renkuResourceUrl,
      executionTimeRecorder,
      ApplicationLogger
    )
}
