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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.jsonld.JsonLD
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult.{DeliverySuccess, RecoverableFailure}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

private trait TriplesUploader[F[_]] {
  def uploadTriples(triples: JsonLD): EitherT[F, RecoverableFailure, DeliverySuccess]
}

private class TriplesUploaderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F],
    retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:    Duration = 6 minutes,
    requestTimeout: Duration = 5 minutes
) extends RdfStoreClientImpl(rdfStoreConfig,
                             timeRecorder,
                             retryInterval = retryInterval,
                             maxRetries = maxRetries,
                             idleTimeoutOverride = idleTimeout.some,
                             requestTimeoutOverride = requestTimeout.some
    )
    with TriplesUploader[F] {

  import TriplesUploadResult._
  import org.http4s.Status._
  import org.http4s.{Request, Response, Status}

  override def uploadTriples(triples: JsonLD): EitherT[F, RecoverableFailure, DeliverySuccess] = EitherT {
    upload[Either[TriplesUploadFailure, DeliverySuccess]](triples)(mapResponse) recoverWith deliveryFailure
  }.leftSemiflatMap(leftOrFail)

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[Either[TriplesUploadFailure, DeliverySuccess]]] = {
    case (Ok, _, _)                         => DeliverySuccess.asRight[TriplesUploadFailure].pure[F]
    case (BadRequest, _, response)          => singleLineBody(response) map toNonRecoverableFailure
    case (InternalServerError, _, response) => singleLineBody(response) map toNonRecoverableFailure
    case (other, _, response)               => singleLineBody(response) map toRecoverableFailure(other)
  }

  private def singleLineBody(response: Response[F]): F[String] = response.as[String].map(LogMessage.toSingleLine)

  private def toNonRecoverableFailure(responseMessage: String): Either[TriplesUploadFailure, DeliverySuccess] =
    NonRecoverableFailure(s"Failed to upload triples $responseMessage").asLeft[DeliverySuccess]

  private def toRecoverableFailure(status: Status)(
      responseMessage:                     String
  ): Either[TriplesUploadFailure, DeliverySuccess] =
    RecoverableFailure(s"$status: $responseMessage").asLeft[DeliverySuccess]

  private def deliveryFailure: PartialFunction[Throwable, F[Either[TriplesUploadFailure, DeliverySuccess]]] = {
    case NonFatal(exception) =>
      RecoverableFailure(exception.getMessage).asLeft[DeliverySuccess].leftWiden[TriplesUploadFailure].pure[F]
  }

  private lazy val leftOrFail: TriplesUploadFailure => F[RecoverableFailure] = {
    case failure: NonRecoverableFailure => failure.raiseError[F, RecoverableFailure]
    case failure: RecoverableFailure    => failure.pure[F]
  }
}

private object TriplesUploader {
  def apply[F[_]: Async: Logger](rdfStoreConfig: RdfStoreConfig,
                                 timeRecorder:   SparqlQueryTimeRecorder[F],
                                 retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
                                 maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
                                 idleTimeout:    Duration = 6 minutes,
                                 requestTimeout: Duration = 5 minutes
  ): F[TriplesUploaderImpl[F]] = MonadThrow[F].catchNonFatal(
    new TriplesUploaderImpl[F](rdfStoreConfig, timeRecorder, retryInterval, maxRetries, idleTimeout, requestTimeout)
  )
}
