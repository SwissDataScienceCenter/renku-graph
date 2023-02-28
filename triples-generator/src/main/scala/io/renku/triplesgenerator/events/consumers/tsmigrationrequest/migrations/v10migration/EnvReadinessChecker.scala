/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.v10migration

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.renku.graph.eventlog.EventLogClient
import io.renku.graph.model.eventlogapi.ServiceStatus
import io.renku.triplesgenerator.events.consumers.awaitinggeneration
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait EnvReadinessChecker[F[_]] {
  def envReadyToTakeEvent: F[Unit]
}

private object EnvReadinessChecker {
  def apply[F[_]: Async: Logger]: F[EnvReadinessChecker[F]] =
    EventLogClient[F].map(new EnvReadinessCheckerImpl[F](_))
}

private class EnvReadinessCheckerImpl[F[_]: Async](elClient: EventLogClient[F], retryTimeout: Duration = 1 second)
    extends EnvReadinessChecker[F] {

  override def envReadyToTakeEvent: F[Unit] =
    waitForTwoSubsequentReadyStatuses(previousCheck = false).void

  private def waitForTwoSubsequentReadyStatuses(previousCheck: Boolean): F[Boolean] =
    checkFreeCapacity >>= {
      case check @ false                  => waitAndTryAgain(check)
      case check @ true if !previousCheck => waitAndTryAgain(check)
      case check @ true                   => check.pure[F]
    }

  private def waitAndTryAgain(previousCheck: Boolean) =
    Temporal[F].delayBy(waitForTwoSubsequentReadyStatuses(previousCheck), retryTimeout)

  private def checkFreeCapacity =
    elClient.getStatus.map(
      _.toEither
        .leftMap(_ => false)
        .map(getAwaitingGenerationFreeCapacity)
        .flatMap(Either.fromOption(_, ifNone = false))
        .map(_ > 0)
        .merge
    )

  private lazy val getAwaitingGenerationFreeCapacity: ServiceStatus => Option[Int] =
    _.subscriptions
      .find(_.name == awaitinggeneration.categoryName)
      .flatMap(_.maybeCapacity)
      .map(_.free)
}
