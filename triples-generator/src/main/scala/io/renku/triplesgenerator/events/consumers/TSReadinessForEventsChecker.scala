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

package io.renku.triplesgenerator.events.consumers

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.events.consumers.EventSchedulingResult
import io.renku.events.consumers.EventSchedulingResult.{Accepted, SchedulingError, ServiceUnavailable}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.consumers.TSStateChecker.TSState
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait TSReadinessForEventsChecker[F[_]] {
  def verifyTSReady: EitherT[F, EventSchedulingResult, Accepted]
}

private object TSReadinessForEventsChecker {
  def apply[F[_]: Async: ReProvisioningStatus: Logger: SparqlQueryTimeRecorder]: F[TSReadinessForEventsChecker[F]] =
    TSStateChecker[F].map(new TSReadinessForEventsCheckerImpl(_))
}

private class TSReadinessForEventsCheckerImpl[F[_]: MonadThrow](tsStateChecker: TSStateChecker[F])
    extends TSReadinessForEventsChecker[F] {

  override def verifyTSReady: EitherT[F, EventSchedulingResult, Accepted] = EitherT {
    tsStateChecker.checkTSState
      .map {
        case TSState.Ready => Accepted.asRight
        case state @ (TSState.MissingDatasets | TSState.ReProvisioning) =>
          ServiceUnavailable(state.show).asLeft.leftWiden[EventSchedulingResult]
      }
      .recover { case NonFatal(exception) =>
        SchedulingError(exception).asLeft[Accepted].leftWiden[EventSchedulingResult]
      }
  }
}
