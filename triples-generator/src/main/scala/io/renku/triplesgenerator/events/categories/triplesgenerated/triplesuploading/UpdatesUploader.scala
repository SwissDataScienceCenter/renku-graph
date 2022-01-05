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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.rdfstore._
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult.{DeliverySuccess, RecoverableFailure}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait UpdatesUploader[F[_]] {
  def send(updateQuery: SparqlQuery): EitherT[F, RecoverableFailure, DeliverySuccess]
}

private class UpdatesUploaderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F],
    retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:    Duration = 5 minutes,
    requestTimeout: Duration = 4 minutes
) extends RdfStoreClientImpl[F](rdfStoreConfig,
                                timeRecorder,
                                retryInterval,
                                maxRetries,
                                idleTimeoutOverride = idleTimeout.some,
                                requestTimeoutOverride = requestTimeout.some
    )
    with UpdatesUploader[F] {

  import LogMessage._
  import TriplesUploadResult._
  import org.http4s.Status.{BadRequest, Ok}
  import org.http4s.{Request, Response, Status}

  import scala.util.control.NonFatal

  override def send(updateQuery: SparqlQuery): EitherT[F, RecoverableFailure, DeliverySuccess] = EitherT {
    updateWitMapping(updateQuery, responseMapper(updateQuery)) recoverWith deliveryFailure
  }.leftSemiflatMap(leftOrFail)

  private def responseMapper(
      updateQuery: SparqlQuery
  ): PartialFunction[(Status, Request[F], Response[F]), F[Either[TriplesUploadFailure, DeliverySuccess]]] = {
    case (Ok, _, _)                => DeliverySuccess.asRight[TriplesUploadFailure].pure[F]
    case (BadRequest, _, response) => (response.as[String] map toSingleLine) map toNonRecoverableFailure(updateQuery)
    case (other, _, response)      => response.as[String] map toSingleLine map toDeliveryFailure(other)
  }

  private def toNonRecoverableFailure(
      updateQuery:   SparqlQuery
  )(responseMessage: String): Either[TriplesUploadFailure, DeliverySuccess] =
    NonRecoverableFailure(s"Triples transformation update '${updateQuery.name}' failed: $responseMessage")
      .asLeft[DeliverySuccess]

  private def toDeliveryFailure(status: Status)(message: String): Either[TriplesUploadFailure, DeliverySuccess] =
    RecoverableFailure(s"Triples transformation update failed: $status: $message").asLeft[DeliverySuccess]

  private def deliveryFailure: PartialFunction[Throwable, F[Either[TriplesUploadFailure, DeliverySuccess]]] = {
    case NonFatal(exception) =>
      RecoverableFailure(exception.getMessage).asLeft[DeliverySuccess].leftWiden[TriplesUploadFailure].pure[F]
  }

  private lazy val leftOrFail: TriplesUploadFailure => F[RecoverableFailure] = {
    case failure: NonRecoverableFailure => failure.raiseError[F, RecoverableFailure]
    case failure: RecoverableFailure    => failure.pure[F]
  }
}
