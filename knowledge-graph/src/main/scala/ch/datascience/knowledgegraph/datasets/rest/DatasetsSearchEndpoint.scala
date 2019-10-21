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

import cats.MonadError
import cats.effect._
import cats.implicits._
import ch.datascience.config.RenkuResourcesUrl
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.knowledgegraph.datasets.CreatorsFinder
import ch.datascience.knowledgegraph.datasets.model.{DatasetCreator, DatasetPublishing}
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.rdfstore.RdfStoreConfig
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.chrisdavenport.log4cats.Logger
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.impl.ValidatingQueryParamDecoderMatcher
import org.http4s.{ParseFailure, QueryParamDecoder, QueryParameterValue, Response}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class DatasetsSearchEndpoint[Interpretation[_]: Effect](
    datasetsFinder:        DatasetsFinder[Interpretation],
    renkuResourcesUrl:     RenkuResourcesUrl,
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import DatasetsFinder.DatasetSearchResult
  import DatasetsSearchEndpoint._
  import ch.datascience.json.JsonOps._
  import ch.datascience.tinytypes.json.TinyTypeEncoders._
  import executionTimeRecorder._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.circe._

  def searchForDatasets(phrase: Phrase): Interpretation[Response[Interpretation]] =
    measureExecutionTime {
      datasetsFinder
        .findDatasets(phrase)
        .flatMap(toHttpResult(phrase))
        .recoverWith(httpResult(phrase))
    } map logExecutionTimeWhen(finishedSuccessfully(phrase))

  private def toHttpResult(phrase: Phrase): List[DatasetSearchResult] => Interpretation[Response[Interpretation]] = {
    case Nil      => NotFound(InfoMessage(s"No datasets matching '$phrase'"))
    case datasets => Ok(datasets.asJson)
  }

  private def httpResult(phrase: Phrase): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Finding datasets matching $phrase' failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(phrase: Phrase): PartialFunction[Response[Interpretation], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      s"Finding datasets containing '$phrase' phrase finished"
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
    case DatasetCreator(maybeEmail, name) =>
      json"""{
        "name": $name
      }""" addIfDefined ("email" -> maybeEmail)
  }
}

object DatasetsSearchEndpoint {

  final class Phrase private (val value: String) extends AnyVal with StringTinyType
  implicit object Phrase extends TinyTypeFactory[Phrase](new Phrase(_)) with NonBlank

  private implicit val queryParameterDecoder: QueryParamDecoder[Phrase] =
    (value: QueryParameterValue) =>
      Phrase.from(value.value).leftMap(_.getMessage).leftMap(ParseFailure(_, "")).toValidatedNel

  object QueryParameter extends ValidatingQueryParamDecoderMatcher[Phrase]("query") {
    val name: String = "query"
  }
}

object IODatasetsSearchEndpoint {

  def apply()(implicit executionContext: ExecutionContext,
              contextShift:              ContextShift[IO],
              timer:                     Timer[IO]): IO[DatasetsSearchEndpoint[IO]] =
    for {
      rdfStoreConfig        <- RdfStoreConfig[IO]()
      renkuBaseUrl          <- RenkuBaseUrl[IO]()
      renkuResourceUrl      <- RenkuResourcesUrl[IO]()
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
    } yield
      new DatasetsSearchEndpoint[IO](
        new IODatasetsFinder(rdfStoreConfig,
                             new CreatorsFinder(rdfStoreConfig, renkuBaseUrl, ApplicationLogger),
                             ApplicationLogger),
        renkuResourceUrl,
        executionTimeRecorder,
        ApplicationLogger
      )
}
