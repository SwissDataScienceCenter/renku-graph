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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.http.client.IORestClient
import ch.datascience.http.client.IORestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.{JsonLDTriples, RdfStoreConfig}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.chrisdavenport.log4cats.Logger
import org.http4s.Uri

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.control.NonFatal

private trait TriplesUploader[Interpretation[_]] {
  def upload(triples: JsonLDTriples): Interpretation[TriplesUploadResult]
}

private sealed trait TriplesUploadResult
private object TriplesUploadResult {
  final case object TriplesUploaded extends TriplesUploadResult
  final case class MalformedTriples(message: String) extends Exception(message) with TriplesUploadResult
  final case class UploadingError(message:   String) extends Exception(message) with TriplesUploadResult
}

private class IOTriplesUploader(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    retryInterval:           FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = MaxRetriesAfterConnectionTimeout
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient[Any](Throttler.noThrottling, logger, retryInterval, maxRetries)
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
    request(POST, uploadUri, rdfStoreConfig.authCredentials)
      .withEntity(triples.value)
      .putHeaders(`Content-Type`(`ld+json`))

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[TriplesUploadResult]] = {
    case (Ok, _, _)                => IO.pure(TriplesUploaded)
    case (BadRequest, _, response) => singleLineBody(response).map(MalformedTriples.apply)
    case (other, _, response)      => singleLineBody(response).map(message => s"$other: $message").map(UploadingError.apply)
  }

  private def singleLineBody(response: Response[IO]): IO[String] =
    response.as[String].map(ExceptionMessage.toSingleLine)

  private lazy val withUploadingError: PartialFunction[Throwable, TriplesUploadResult] = {
    case NonFatal(exception) => UploadingError(exception.getMessage)
  }
}

private object IOTriplesUploader {
  def apply()(implicit executionContext: ExecutionContext,
              contextShift:              ContextShift[IO],
              timer:                     Timer[IO]): IO[IOTriplesUploader] =
    RdfStoreConfig[IO]() map (new IOTriplesUploader(_, ApplicationLogger))
}
