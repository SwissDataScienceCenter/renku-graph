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

package io.renku.rdfstore

import cats.MonadError
import cats.effect._
import cats.effect.kernel.Temporal
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.control.Throttler
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.http.client.{HttpRequest, RestClient}
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.PagingRequest
import org.http4s.MediaRange._
import org.http4s.{MediaType, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

abstract class RdfStoreClientImpl[Interpretation[_]: Async: Temporal: Logger](
    rdfStoreConfig:         RdfStoreConfig,
    timeRecorder:           SparqlQueryTimeRecorder[Interpretation],
    retryInterval:          FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeoutOverride:    Option[Duration] = None,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient[Interpretation, RdfStoreClientImpl[Interpretation]](Throttler.noThrottling,
                                                                         Some(timeRecorder.instance),
                                                                         retryInterval,
                                                                         maxRetries,
                                                                         idleTimeoutOverride,
                                                                         requestTimeoutOverride
    ) {

  import RdfStoreClientImpl._
  import io.renku.http.client.UrlEncoder.urlEncode
  import org.http4s.MediaType.application._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.circe.jsonOf
  import org.http4s.headers._
  import org.http4s.{Request, Response, Status}
  import rdfStoreConfig._

  protected def updateWithNoResult(using: SparqlQuery): Interpretation[Unit] =
    updateWitMapping[Unit](using, toFullResponseMapper(_ => ().pure[Interpretation]))

  protected def updateWitMapping[ResultType](
      using: SparqlQuery,
      mapResponse: PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[
        ResultType
      ]]
  ): Interpretation[ResultType] = runQuery(using, mapResponse, RdfUpdate)

  protected def queryExpecting[ResultType](
      using:          SparqlQuery
  )(implicit decoder: Decoder[ResultType]): Interpretation[ResultType] =
    runQuery(
      using,
      toFullResponseMapper(responseMapperFor[ResultType]),
      RdfQuery
    )

  private def runQuery[ResultType](
      query: SparqlQuery,
      mapResponse: PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[
        ResultType
      ]],
      queryType: RdfQueryType
  ): Interpretation[ResultType] =
    for {
      uri    <- validateUri((fusekiBaseUrl / datasetName / path(queryType)).toString)
      result <- send(sparqlQueryRequest(uri, queryType, query))(mapResponse)
    } yield result

  private def sparqlQueryRequest(uri: Uri, queryType: RdfQueryType, query: SparqlQuery) = HttpRequest(
    request(POST, uri, rdfStoreConfig.authCredentials)
      .withEntity(toEntity(queryType, query))
      .putHeaders(`Content-Type`(`x-www-form-urlencoded`),
                  Accept(new MediaType(`application/*`.mainType, "sparql-results+json"))
      ),
    name = query.name
  )

  private def toFullResponseMapper[ResultType](
      mapResponse: Response[Interpretation] => Interpretation[ResultType]
  ): PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[ResultType]] = {
    case (Ok, _, response) =>
      mapResponse(response)
  }

  private def responseMapperFor[ResultType](implicit
      decoder: Decoder[ResultType]
  ): Response[Interpretation] => Interpretation[ResultType] =
    _.as[ResultType](implicitly[MonadError[Interpretation, Throwable]], jsonOf[Interpretation, ResultType])

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
  )(implicit decoder:  Decoder[ResultType]): PagedResultsFinder[Interpretation, ResultType] =
    new PagedResultsFinder[Interpretation, ResultType] {

      import io.renku.http.rest.paging.model.Total
      import io.renku.tinytypes.json.TinyTypeDecoders._

      override def findResults(pagingRequest: PagingRequest): Interpretation[List[ResultType]] =
        for {
          queryWithPaging <- query.include[Interpretation](pagingRequest)
          results         <- queryExpecting[List[ResultType]](using = queryWithPaging)
        } yield results

      override def findTotal() =
        queryExpecting[Option[Total]](using = (maybeCountQuery getOrElse query).toCountQuery).flatMap {
          case Some(total) => total.pure[Interpretation]
          case None        => new Exception("Total number of records cannot be found").raiseError[Interpretation, Total]
        }

      private implicit val totalDecoder: Decoder[Option[Total]] = {
        val totals: Decoder[Total] = _.downField("total").downField("value").as[Total]
        _.downField("results").downField("bindings").as(decodeList(totals)).map(_.headOption)
      }

      private implicit val recordsDecoder: Decoder[List[ResultType]] =
        _.downField("results").downField("bindings").as(decodeList[ResultType])
    }
}

object RdfStoreClientImpl {

  private trait RdfQueryType
  private final implicit case object RdfQuery extends RdfQueryType
  private type RdfQuery = RdfQuery.type
  private final implicit case object RdfUpdate extends RdfQueryType
}
