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

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.jsonld.JsonLD
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.RecoverableErrorsRecovery
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait TriplesUploader[F[_]] {
  def uploadTriples(triples: JsonLD): EitherT[F, ProcessingRecoverableError, Unit]
}

private class TriplesUploaderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig:   RdfStoreConfig,
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery,
    retryInterval:    FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:       Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:      Duration = 11 minutes,
    requestTimeout:   Duration = 10 minutes
) extends RdfStoreClientImpl(rdfStoreConfig,
                             retryInterval = retryInterval,
                             maxRetries = maxRetries,
                             idleTimeoutOverride = idleTimeout.some,
                             requestTimeoutOverride = requestTimeout.some
    )
    with TriplesUploader[F] {

  import org.http4s.Status._
  import org.http4s.{Request, Response, Status}

  override def uploadTriples(triples: JsonLD): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    uploadAndMap[Either[ProcessingRecoverableError, Unit]](triples)(
      mapResponse
    ) recoverWith recoveryStrategy.maybeRecoverableError
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[Either[ProcessingRecoverableError, Unit]]] = {
    case (Ok, _, _) => ().asRight[ProcessingRecoverableError].pure[F]
  }
}

private object TriplesUploader {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](rdfStoreConfig: RdfStoreConfig,
                                                          retryInterval: FiniteDuration = SleepAfterConnectionIssue,
                                                          maxRetries: Int Refined NonNegative =
                                                            MaxRetriesAfterConnectionTimeout,
                                                          idleTimeout:    Duration = 6 minutes,
                                                          requestTimeout: Duration = 5 minutes
  ): F[TriplesUploaderImpl[F]] = MonadThrow[F].catchNonFatal(
    new TriplesUploaderImpl[F](rdfStoreConfig,
                               RecoverableErrorsRecovery,
                               retryInterval,
                               maxRetries,
                               idleTimeout,
                               requestTimeout
    )
  )
}
