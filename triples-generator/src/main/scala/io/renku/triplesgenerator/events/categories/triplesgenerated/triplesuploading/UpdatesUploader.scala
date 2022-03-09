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
import io.renku.http.client.RestClientError.BadRequestException
import io.renku.rdfstore._
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.RecoverableErrorsRecovery
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult.NonRecoverableFailure
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait UpdatesUploader[F[_]] {
  def send(updateQuery: SparqlQuery): EitherT[F, ProcessingRecoverableError, Unit]
}

private class UpdatesUploaderImpl[F[_]: Async: Logger](
    rdfStoreConfig:   RdfStoreConfig,
    timeRecorder:     SparqlQueryTimeRecorder[F],
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery,
    retryInterval:    FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:       Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:      Duration = 5 minutes,
    requestTimeout:   Duration = 4 minutes
) extends RdfStoreClientImpl[F](rdfStoreConfig,
                                timeRecorder,
                                retryInterval,
                                maxRetries,
                                idleTimeoutOverride = idleTimeout.some,
                                requestTimeoutOverride = requestTimeout.some
    )
    with UpdatesUploader[F] {

  import org.http4s.Status.Ok
  import org.http4s.{Request, Response, Status}
  import recoveryStrategy._

  override def send(updateQuery: SparqlQuery): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    updateWitMapping(updateQuery, responseMapper).recoverWith(
      maybeRecoverableError[F, Unit](
        s"Triples transformation update '${updateQuery.name}' failed"
      ) orElse toNonRecoverableFailure(updateQuery)
    )
  }

  private lazy val responseMapper
      : PartialFunction[(Status, Request[F], Response[F]), F[Either[ProcessingRecoverableError, Unit]]] = {
    case (Ok, _, _) => ().asRight[ProcessingRecoverableError].pure[F]
  }

  private def toNonRecoverableFailure(
      updateQuery: SparqlQuery
  ): PartialFunction[Throwable, F[Either[ProcessingRecoverableError, Unit]]] = { case error: BadRequestException =>
    NonRecoverableFailure(s"Triples transformation update '${updateQuery.name}' failed", error)
      .raiseError[F, Either[ProcessingRecoverableError, Unit]]
  }
}
