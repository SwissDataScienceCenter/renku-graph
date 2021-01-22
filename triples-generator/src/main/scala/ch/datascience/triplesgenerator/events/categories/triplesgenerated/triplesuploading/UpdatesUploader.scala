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

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.http.client.IORestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import ch.datascience.rdfstore._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

private trait UpdatesUploader[Interpretation[_]] {
  def send(updateQuery: SparqlQuery): Interpretation[TriplesUploadResult]
}

private class IOUpdatesUploader(
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
) extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder, retryInterval, maxRetries)
    with UpdatesUploader[IO] {

  import LogMessage._
  import TriplesUploadResult._
  import cats.syntax.all._
  import org.http4s.Status.{BadRequest, Ok}
  import org.http4s.{Request, Response, Status}

  import scala.util.control.NonFatal

  override def send(updateQuery: SparqlQuery): IO[TriplesUploadResult] =
    updateWitMapping(updateQuery, responseMapper(updateQuery)) recoverWith deliveryFailure

  private def responseMapper(
      updateQuery: SparqlQuery
  ): PartialFunction[(Status, Request[IO], Response[IO]), IO[TriplesUploadResult]] = {
    case (Ok, _, _)                => IO.pure(DeliverySuccess)
    case (BadRequest, _, response) => response.as[String] map toSingleLine map toInvalidUpdatesFailure(updateQuery)
    case (other, _, response)      => response.as[String] map toSingleLine map toDeliveryFailure(other)
  }

  private def toInvalidUpdatesFailure(updateQuery: SparqlQuery)(responseMessage: String) =
    InvalidUpdatesFailure(s"Triples curation update '${updateQuery.name}' failed: $responseMessage")

  private def toDeliveryFailure(status: Status)(message: String) =
    RecoverableFailure(s"Triples curation update failed: $status: $message")

  private def deliveryFailure: PartialFunction[Throwable, IO[TriplesUploadResult]] = { case NonFatal(exception) =>
    ME.pure(RecoverableFailure(exception.getMessage))
  }
}
