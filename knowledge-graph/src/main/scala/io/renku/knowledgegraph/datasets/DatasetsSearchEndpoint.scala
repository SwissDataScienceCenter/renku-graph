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

import DatasetsSearchEndpoint.Query._
import DatasetsSearchEndpoint.Sort
import cats.effect._
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.config._
import io.renku.config.renku.ResourceUrl
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabUrl
import io.renku.http.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.server.security.model.AuthUser
import io.renku.logging.ExecutionTimeRecorder
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.triplesstore.{RenkuConnectionConfig, SparqlQueryTimeRecorder}
import org.http4s._
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.impl.OptionalValidatingQueryParamDecoderMatcher
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait DatasetsSearchEndpoint[F[_]] {
  def searchForDatasets(maybePhrase: Option[Phrase],
                        sort:        Sort.By,
                        paging:      PagingRequest,
                        maybeUser:   Option[AuthUser]
  ): F[Response[F]]
}

class DatasetsSearchEndpointImpl[F[_]: Parallel: MonadThrow: Logger](
    datasetsFinder:        DatasetsFinder[F],
    renkuApiUrl:           renku.ApiUrl,
    gitLabUrl:             GitLabUrl,
    executionTimeRecorder: ExecutionTimeRecorder[F]
) extends Http4sDsl[F]
    with DatasetsSearchEndpoint[F] {

  import DatasetSearchResult._
  import PagingRequest.Decoders._
  import executionTimeRecorder._

  private implicit lazy val apiUrl: renku.ApiUrl = renkuApiUrl
  private implicit lazy val glUrl:  GitLabUrl    = gitLabUrl

  def searchForDatasets(maybePhrase: Option[Phrase],
                        sort:        Sort.By,
                        paging:      PagingRequest,
                        maybeUser:   Option[AuthUser]
  ): F[Response[F]] = measureExecutionTime {
    implicit val datasetsUrl: renku.ResourceUrl = requestedUrl(maybePhrase, sort, paging)
    datasetsFinder
      .findDatasets(maybePhrase, sort, paging, maybeUser)
      .map(_.toHttpResponse[F, renku.ResourceUrl])
      .recoverWith(httpResult(maybePhrase))
  } map logExecutionTimeWhen(finishedSuccessfully(maybePhrase))

  private def requestedUrl(maybePhrase: Option[Phrase], sort: Sort.By, paging: PagingRequest): renku.ResourceUrl =
    (renkuApiUrl / "datasets") ? (page.parameterName -> paging.page) & (perPage.parameterName -> paging.perPage) & (Sort.sort.parameterName -> sort) && (query.parameterName -> maybePhrase)

  private def httpResult(
      maybePhrase: Option[Phrase]
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(
      maybePhrase
        .map(phrase => s"Finding datasets matching '$phrase' failed")
        .getOrElse("Finding all datasets failed")
    )
    Logger[F].error(exception)(errorMessage.value) >>
      InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(maybePhrase: Option[Phrase]): PartialFunction[Response[F], String] = {
    case response if response.status == Ok =>
      maybePhrase
        .map(phrase => s"Finding datasets containing '$phrase' phrase finished")
        .getOrElse("Finding all datasets finished")
  }
}

object DatasetsSearchEndpoint {

  def apply[F[_]: Parallel: Async: Logger: SparqlQueryTimeRecorder]: F[DatasetsSearchEndpoint[F]] = for {
    renkuConnectionConfig <- RenkuConnectionConfig[F]()
    renkuResourceUrl      <- renku.ApiUrl[F]()
    gitLabUrl             <- GitLabUrlLoader[F]()
    executionTimeRecorder <- ExecutionTimeRecorder[F]()
    creatorsFinder        <- CreatorsFinder(renkuConnectionConfig)
    datasetsFinder        <- DatasetsFinder(renkuConnectionConfig, creatorsFinder)
  } yield new DatasetsSearchEndpointImpl[F](datasetsFinder, renkuResourceUrl, gitLabUrl, executionTimeRecorder)

  object Query {
    final class Phrase private (val value: String) extends AnyVal with StringTinyType
    implicit object Phrase                         extends TinyTypeFactory[Phrase](new Phrase(_)) with NonBlank[Phrase]

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

  object Sort extends io.renku.http.rest.SortBy {

    type PropertyType = SearchProperty

    sealed abstract class SearchProperty(override val name: String) extends Property(name)

    final case object TitleProperty         extends SearchProperty("title")
    final case object DateProperty          extends SearchProperty("date")
    final case object DatePublishedProperty extends SearchProperty("datePublished")
    final case object ProjectsCountProperty extends SearchProperty("projectsCount")

    override lazy val properties: Set[SearchProperty] = Set(
      TitleProperty,
      DateProperty,
      DatePublishedProperty,
      ProjectsCountProperty
    )
  }
}
