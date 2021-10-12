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

import cats.effect.{ConcurrentEffect, Timer}
import cats.syntax.all._
import ch.datascience.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import ch.datascience.rdfstore._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

private trait UpdatesUploader[Interpretation[_]] {
  def send(updateQuery: SparqlQuery): Interpretation[TriplesUploadResult]
}

private class UpdatesUploaderImpl[Interpretation[_]: ConcurrentEffect: Timer](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[Interpretation],
    retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:    Duration = 5 minutes,
    requestTimeout: Duration = 4 minutes
)(implicit
    executionContext: ExecutionContext,
    logger:           Logger[Interpretation]
) extends RdfStoreClientImpl[Interpretation](rdfStoreConfig,
                                             logger,
                                             timeRecorder,
                                             retryInterval,
                                             maxRetries,
                                             idleTimeoutOverride = idleTimeout.some,
                                             requestTimeoutOverride = requestTimeout.some
    )
    with UpdatesUploader[Interpretation] {

  import LogMessage._
  import TriplesUploadResult._
  import org.http4s.Status.{BadRequest, Ok}
  import org.http4s.{Request, Response, Status}

  import scala.util.control.NonFatal

  override def send(updateQuery: SparqlQuery): Interpretation[TriplesUploadResult] =
    updateWitMapping(updateQuery, responseMapper(updateQuery)) recoverWith deliveryFailure

  private def responseMapper(
      updateQuery: SparqlQuery
  ): PartialFunction[(Status, Request[Interpretation], Response[Interpretation]),
                     Interpretation[TriplesUploadResult]
  ] = {
    case (Ok, _, _) => DeliverySuccess.pure[Interpretation].widen[TriplesUploadResult]
    case (BadRequest, _, response) =>
      response.as[String] map toSingleLine map toInvalidUpdatesFailure(updateQuery)
    case (other, _, response) =>
      response.as[String] map toSingleLine map toDeliveryFailure(other)
  }

  private def toInvalidUpdatesFailure(updateQuery: SparqlQuery)(responseMessage: String): TriplesUploadResult =
    InvalidUpdatesFailure(s"Triples curation update '${updateQuery.name}' failed: $responseMessage")

  private def toDeliveryFailure(status: Status)(message: String): TriplesUploadResult =
    RecoverableFailure(s"Triples curation update failed: $status: $message")

  private def deliveryFailure: PartialFunction[Throwable, Interpretation[TriplesUploadResult]] = {
    case NonFatal(exception) =>
      RecoverableFailure(exception.getMessage).pure[Interpretation].widen[TriplesUploadResult]
  }
}
