/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore

import cats.{Applicative, MonadThrow}
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.control.Throttler
import io.renku.http.client.{HttpRequest, RestClient}
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.PagingRequest
import io.renku.jsonld.JsonLD
import org.http4s.{MediaType, Uri}
import org.http4s.MediaRange._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait TSClient[F[_]] {
  def updateWithNoResult(updateQuery:         SparqlQuery): F[Unit]
  def queryExpecting[ResultType](selectQuery: SparqlQuery)(implicit decoder: Decoder[ResultType]): F[ResultType]
  def upload(jsonLD:                          JsonLD): F[Unit]

  def pagedResultsFinder[ResultType](
      query:           SparqlQuery,
      maybeCountQuery: Option[SparqlQuery] = None
  )(implicit decoder: Decoder[ResultType]): PagedResultsFinder[F, ResultType]
}

object TSClient {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      triplesStoreConfig: DatasetConnectionConfig,
      retryInterval:      FiniteDuration = SleepAfterConnectionIssue,
      maxRetries:         Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
      timeout:            Duration = 20.minutes
  ): TSClient[F] =
    new TSClientImpl[F](triplesStoreConfig, retryInterval, maxRetries, timeout)
}

class TSClientImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    triplesStoreConfig: DatasetConnectionConfig,
    retryInterval:      FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:         Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    timeout:            Duration = 20.minutes,
    printQueries:       Boolean = false
) extends RestClient(
      Throttler.noThrottling,
      Option(implicitly[SparqlQueryTimeRecorder[F]].instance),
      retryInterval,
      maxRetries,
      idleTimeoutOverride = (timeout * 1.1).some,
      requestTimeoutOverride = timeout.some
    )
    with TSClient[F]
    with RdfMediaTypes {

  private val applicative: Applicative[F] = Applicative[F]
  import TSClientImpl._
  import applicative.whenA
  import eu.timepit.refined.auto._
  import io.renku.http.client.UrlEncoder.urlEncode
  import org.http4s.{Request, Response, Status}
  import org.http4s.MediaType.application._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.headers._
  import org.http4s.circe._
  import triplesStoreConfig._

  override def updateWithNoResult(updateQuery: SparqlQuery): F[Unit] =
    updateWitMapping[Unit](updateQuery, toFullResponseMapper(_ => ().pure[F]))

  protected def updateWitMapping[ResultType](
      updateQuery: SparqlQuery,
      mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[ResultType]]
  ): F[ResultType] = runQuery(updateQuery, mapResponse, SparqlUpdate)

  override def queryExpecting[ResultType](
      selectQuery: SparqlQuery
  )(implicit decoder: Decoder[ResultType]): F[ResultType] =
    runQuery(
      selectQuery,
      toFullResponseMapper(responseMapperFor[ResultType]),
      SparqlSelect
    )

  override def upload(jsonLD: JsonLD): F[Unit] = uploadAndMap[Unit](jsonLD)(jsonUploadMapResponse)

  protected def uploadAndMap[ResultType](jsonLD: JsonLD)(mapResponse: ResponseMapping[ResultType]): F[ResultType] =
    for {
      uri          <- validateUri((fusekiUrl / datasetName / "data").toString)
      uploadResult <- send(uploadRequest(uri, jsonLD))(mapResponse)
    } yield uploadResult

  private def uploadRequest(uploadUri: Uri, jsonLD: JsonLD) = HttpRequest(
    request(POST, uploadUri, triplesStoreConfig.authCredentials)
      .withEntity(jsonLD.toJson)
      .putHeaders(`Content-Type`(`ld+json`)),
    name = "json-ld upload"
  )

  private lazy val jsonUploadMapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Unit]] = {
    case (Ok, _, _) => ().pure[F]
  }

  private def runQuery[ResultType](
      query:       SparqlQuery,
      mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[ResultType]],
      queryType:   SparqlQueryType
  ): F[ResultType] = for {
    uri    <- validateUri((fusekiUrl / datasetName / path(queryType)).toString)
    _      <- log(query)
    result <- send(sparqlQueryRequest(uri, queryType, query))(mapResponse)
  } yield result

  private def log(query: SparqlQuery) = whenA(printQueries)(println(query.show).pure[F])

  private def sparqlQueryRequest(uri: Uri, queryType: SparqlQueryType, query: SparqlQuery) = HttpRequest(
    request(POST, uri, triplesStoreConfig.authCredentials)
      .withEntity(toEntity(queryType, query))
      .putHeaders(`Content-Type`(`x-www-form-urlencoded`),
                  Accept(new MediaType(`application/*`.mainType, "sparql-results+json"))
      ),
    name = query.name
  )

  private def toFullResponseMapper[ResultType](
      mapResponse: Response[F] => F[ResultType]
  ): PartialFunction[(Status, Request[F], Response[F]), F[ResultType]] = { case (Ok, _, response) =>
    mapResponse(response)
  }

  private def responseMapperFor[ResultType](implicit
      decoder: Decoder[ResultType]
  ): Response[F] => F[ResultType] = _.as[ResultType](MonadThrow[F], jsonOf[F, ResultType])

  private def toEntity(queryType: SparqlQueryType, query: SparqlQuery): String =
    queryType match {
      case _: SparqlSelect => s"query=${urlEncode(query.toString)}"
      case _ => s"update=${urlEncode(query.toString)}"
    }

  private def path(queryType: SparqlQueryType): String = queryType match {
    case _: SparqlSelect => "sparql"
    case _ => "update"
  }

  final def pagedResultsFinder[ResultType](
      query:           SparqlQuery,
      maybeCountQuery: Option[SparqlQuery] = None
  )(implicit decoder: Decoder[ResultType]): PagedResultsFinder[F, ResultType] =
    new PagedResultsFinder[F, ResultType] {

      import io.renku.http.rest.paging.model.Total
      import io.renku.tinytypes.json.TinyTypeDecoders._

      override def findResults(pagingRequest: PagingRequest): F[List[ResultType]] = for {
        queryWithPaging <- query.include[F](pagingRequest)
        results         <- queryExpecting[List[ResultType]](selectQuery = queryWithPaging)
      } yield results

      override def findTotal(): F[Total] =
        queryExpecting[Option[Total]](selectQuery = (maybeCountQuery getOrElse query).toCountQuery).flatMap {
          case Some(total) => total.pure[F]
          case None        => new Exception("Total number of records cannot be found").raiseError[F, Total]
        }

      private implicit val totalDecoder: Decoder[Option[Total]] = {
        val totals: Decoder[Total] = _.downField("total").downField("value").as[Total]
        _.downField("results").downField("bindings").as(decodeList(totals)).map(_.headOption)
      }

      private implicit val recordsDecoder: Decoder[List[ResultType]] =
        _.downField("results").downField("bindings").as(decodeList[ResultType])
    }
}

object TSClientImpl {

  private trait SparqlQueryType
  private final implicit case object SparqlSelect extends SparqlQueryType
  private type SparqlSelect = SparqlSelect.type
  private final implicit case object SparqlUpdate extends SparqlQueryType
}
