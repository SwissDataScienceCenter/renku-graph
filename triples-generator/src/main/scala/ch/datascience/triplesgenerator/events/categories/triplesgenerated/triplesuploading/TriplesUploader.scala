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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.http.client.IORestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import ch.datascience.http.client.{HttpRequest, IORestClient}
import ch.datascience.rdfstore.{JsonLDTriples, RdfStoreConfig, SparqlQueryTimeRecorder}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import org.http4s.Uri
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
//import scala.language.postfixOps
import scala.util.control.NonFatal

private trait TriplesUploader[Interpretation[_]] {
  def upload(triples: JsonLDTriples): Interpretation[TriplesUploadResult]
}

private class IOTriplesUploader(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO],
    retryInterval:           FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:             Duration = 6 minutes,
    requestTimeout:          Duration = 5 minutes
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient[Any](Throttler.noThrottling,
                              logger,
                              maybeTimeRecorder = timeRecorder.instance.some,
                              retryInterval = retryInterval,
                              maxRetries = maxRetries,
                              idleTimeoutOverride = idleTimeout.some,
                              requestTimeoutOverride = requestTimeout.some
    )
    with TriplesUploader[IO] {

  import TriplesUploadResult._
  import org.http4s.MediaType.application._
  import org.http4s.Method.POST
  import org.http4s.Status._
  import org.http4s.circe._
  import org.http4s.headers._
  import org.http4s.{Request, Response, Status}

  private lazy val dataUploadUrl = rdfStoreConfig.fusekiBaseUrl / rdfStoreConfig.datasetName / "data"

  def upload(triples: JsonLDTriples): IO[TriplesUploadResult] = {
    for {
      uri          <- validateUri(dataUploadUrl.value)
      uploadResult <- send(uploadRequest(uri, triples))(mapResponse)
    } yield uploadResult
  } recover withUploadingError

  private def uploadRequest(uploadUri: Uri, triples: JsonLDTriples) =
    HttpRequest(
      request(POST, uploadUri, rdfStoreConfig.authCredentials)
        .withEntity(triples.value)
        .putHeaders(`Content-Type`(`ld+json`)),
      name = "json-ld upload"
    )

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[TriplesUploadResult]] = {
    case (Ok, _, _)                         => IO.pure(DeliverySuccess)
    case (BadRequest, _, response)          => singleLineBody(response).map(InvalidTriplesFailure.apply)
    case (InternalServerError, _, response) => singleLineBody(response).map(InvalidTriplesFailure.apply)
    case (other, _, response) =>
      singleLineBody(response).map(message => s"$other: $message").map(RecoverableFailure.apply)
  }

  private def singleLineBody(response: Response[IO]): IO[String] =
    response.as[String].map(LogMessage.toSingleLine)

  private lazy val withUploadingError: PartialFunction[Throwable, TriplesUploadResult] = { case NonFatal(exception) =>
    RecoverableFailure(exception.getMessage)
  }
}
