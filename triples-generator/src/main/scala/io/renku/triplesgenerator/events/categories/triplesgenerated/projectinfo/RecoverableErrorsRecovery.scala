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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.MonadThrow
import cats.syntax.all._
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import org.http4s.Status.{Forbidden, ServiceUnavailable, Unauthorized}

private trait RecoverableErrorsRecovery {

  private type RecoveryStrategy[F[_], OUT] = PartialFunction[Throwable, F[Either[ProcessingRecoverableError, OUT]]]

  def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = {
    case exception @ (_: ConnectivityException | _: ClientException) =>
      TransformationRecoverableError(exception.getMessage, exception.getCause)
        .asLeft[OUT]
        .leftWiden[ProcessingRecoverableError]
        .pure[F]
    case exception @ UnexpectedResponseException(ServiceUnavailable | Forbidden | Unauthorized, _) =>
      TransformationRecoverableError(exception.getMessage, exception.getCause)
        .asLeft[OUT]
        .leftWiden[ProcessingRecoverableError]
        .pure[F]
  }
}

private object RecoverableErrorsRecovery extends RecoverableErrorsRecovery
