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

package ch.datascience.rdfstore

import cats.MonadError
import cats.effect._
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.http.client.IORestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import ch.datascience.http.client.{HttpRequest, IORestClient}
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.PagingRequest
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import org.typelevel.log4cats.Logger
import io.circe.Decoder
import io.circe.Decoder.decodeList
import org.http4s.{Header, Uri}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

abstract class IORdfStoreClient(
    rdfStoreConfig: RdfStoreConfig,
    logger:         Logger[IO],
    timeRecorder:   SparqlQueryTimeRecorder[IO],
    retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO],
    ME:               MonadError[IO, Throwable]
) extends IORestClient(Throttler.noThrottling, logger, Some(timeRecorder.instance), retryInterval, maxRetries) {

  import IORdfStoreClient._
  import ch.datascience.http.client.UrlEncoder.urlEncode
  import org.http4s.MediaType.application._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.circe.jsonOf
  import org.http4s.headers._
  import org.http4s.{Request, Response, Status}
  import rdfStoreConfig._

  protected def updateWithNoResult(using: SparqlQuery): IO[Unit] =
    updateWitMapping[Unit](using, toFullResponseMapper(_ => IO.unit))

  protected def updateWitMapping[ResultType](
      using:       SparqlQuery,
      mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]]
  ): IO[ResultType] = runQuery(using, mapResponse, RdfUpdate)

  protected def queryExpecting[ResultType](using: SparqlQuery)(implicit decoder: Decoder[ResultType]): IO[ResultType] =
    runQuery(
      using,
      toFullResponseMapper(responseMapperFor[ResultType]),
      RdfQuery
    )

  private def runQuery[ResultType](
      query:       SparqlQuery,
      mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]],
      queryType:   RdfQueryType
  ): IO[ResultType] =
    for {
      uri    <- validateUri((fusekiBaseUrl / datasetName / path(queryType)).toString)
      result <- send(sparqlQueryRequest(uri, queryType, query))(mapResponse)
    } yield result

  private def sparqlQueryRequest(uri: Uri, queryType: RdfQueryType, query: SparqlQuery) = HttpRequest(
    request(POST, uri, rdfStoreConfig.authCredentials)
      .withEntity(toEntity(queryType, query))
      .putHeaders(`Content-Type`(`x-www-form-urlencoded`),
                  Header(Accept.name.value, "application/sparql-results+json")
      ),
    name = query.name
  )

  private def toFullResponseMapper[ResultType](
      mapResponse: Response[IO] => IO[ResultType]
  ): PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]] = { case (Ok, _, response) =>
    mapResponse(response)
  }

  private def responseMapperFor[ResultType](implicit decoder: Decoder[ResultType]): Response[IO] => IO[ResultType] =
    _.as[ResultType](implicitly[MonadError[IO, Throwable]], jsonOf[IO, ResultType])

  private def toEntity(queryType: RdfQueryType, query: SparqlQuery): String = queryType match {
    case _: RdfQuery => s"query=${urlEncode(query.toString)}"
    case _ => s"update=${urlEncode(query.toString)}"
  }

  private def path(queryType: RdfQueryType): String = queryType match {
    case _: RdfQuery => "sparql"
    case _ => "update"
  }

  protected def pagedResultsFinder[ResultType](
      query:           SparqlQuery,
      maybeCountQuery: Option[SparqlQuery] = None
  )(implicit decoder:  Decoder[ResultType]): PagedResultsFinder[IO, ResultType] =
    new PagedResultsFinder[IO, ResultType] {

      import ch.datascience.http.rest.paging.model.Total
      import ch.datascience.tinytypes.json.TinyTypeDecoders._

      override def findResults(pagingRequest: PagingRequest): IO[List[ResultType]] =
        for {
          queryWithPaging <- query.include[IO](pagingRequest)
          results         <- queryExpecting[List[ResultType]](using = queryWithPaging)
        } yield results

      override def findTotal() =
        queryExpecting[Option[Total]](using = (maybeCountQuery getOrElse query).toCountQuery).flatMap {
          case Some(total) => total.pure[IO]
          case None        => new Exception("Total number of records cannot be found").raiseError[IO, Total]
        }

      private implicit val totalDecoder: Decoder[Option[Total]] = {
        val totals: Decoder[Total] = _.downField("total").downField("value").as[Total]
        _.downField("results").downField("bindings").as(decodeList(totals)).map(_.headOption)
      }

      private implicit val recordsDecoder: Decoder[List[ResultType]] =
        _.downField("results").downField("bindings").as(decodeList[ResultType])
    }
}

object IORdfStoreClient {

  private trait RdfQueryType
  private final implicit case object RdfQuery extends RdfQueryType
  private type RdfQuery = RdfQuery.type
  private final implicit case object RdfUpdate extends RdfQueryType
}
