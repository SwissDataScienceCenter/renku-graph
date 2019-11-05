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

package ch.datascience.rdfstore

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.http.client.IORestClient
import ch.datascience.rdfstore.IORdfStoreClient.RdfQueryType
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder
import org.http4s.{Header, Uri}

import scala.concurrent.ExecutionContext

abstract class IORdfStoreClient(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext,
  contextShift:              ContextShift[IO],
  timer:                     Timer[IO],
  ME:                        MonadError[IO, Throwable])
    extends IORestClient(Throttler.noThrottling, logger) {

  import ch.datascience.rdfstore.IORdfStoreClient.{Query, RdfQuery, RdfUpdate}
  import org.http4s.MediaType.application._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.circe.jsonOf
  import org.http4s.headers._
  import org.http4s.{Request, Response, Status}
  import rdfStoreConfig._

  protected def queryWitNoResult(using: String): IO[Unit] =
    runQuery(using, (_: Response[IO]) => IO.unit, RdfUpdate)

  protected def queryExpecting[ResultType](using: String)(implicit decoder: Decoder[ResultType]): IO[ResultType] =
    runQuery(using, responseMapperFor[ResultType], RdfQuery)

  private def runQuery[ResultType](
      using:       String,
      mapResponse: Response[IO] => IO[ResultType],
      queryType:   RdfQueryType
  ): IO[ResultType] =
    for {
      uri    <- validateUri((fusekiBaseUrl / datasetName / path(queryType)).toString)
      query  <- ME.fromEither(Query.from(using))
      result <- send(uploadRequest(uri, queryType, query))(toFullResponseMapper(mapResponse))
    } yield result

  private def uploadRequest(uri: Uri, queryType: RdfQueryType, query: Query): Request[IO] =
    request(POST, uri, rdfStoreConfig.authCredentials)
      .withEntity(toEntity(queryType, query))
      .putHeaders(`Content-Type`(`x-www-form-urlencoded`), Header(Accept.name.value, "application/sparql-results+json"))

  private def toFullResponseMapper[ResultType](
      mapResponse: Response[IO] => IO[ResultType]
  ): PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]] = {
    case (Ok, _, response) => mapResponse(response)
  }

  private def responseMapperFor[ResultType](implicit decoder: Decoder[ResultType]): Response[IO] => IO[ResultType] =
    _.as[ResultType](implicitly[MonadError[IO, Throwable]], jsonOf[IO, ResultType])

  private def toEntity(queryType: RdfQueryType, query: Query): String = queryType match {
    case _: RdfQuery => s"query=$query"
    case _ => s"update=$query"
  }

  private def path(queryType: RdfQueryType): String = queryType match {
    case _: RdfQuery => "sparql"
    case _ => "update"
  }
}

object IORdfStoreClient {

  class Query private (val value: String) extends AnyVal with StringTinyType
  object Query extends TinyTypeFactory[Query](new Query(_)) with NonBlank

  private trait RdfQueryType
  private final implicit case object RdfQuery extends RdfQueryType
  private type RdfQuery = RdfQuery.type
  private final implicit case object RdfUpdate extends RdfQueryType
  private type RdfUpdate = RdfUpdate.type
}
