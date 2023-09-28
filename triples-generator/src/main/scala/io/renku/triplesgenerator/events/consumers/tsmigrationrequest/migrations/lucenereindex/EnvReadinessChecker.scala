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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.lucenereindex

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.renku.eventlog.api.EventLogClient
import io.renku.graph.model.eventlogapi.ServiceStatus
import io.renku.triplesgenerator.events.consumers.triplesgenerated
import org.typelevel.log4cats.Logger

import scala.collection.immutable.Queue
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

  private val consecutiveSuccessesToPass = 3

  override def envReadyToTakeEvent: F[Unit] =
    waitForSubsequentReadyStatuses(previousChecks = Queue.empty).void

  private def waitForSubsequentReadyStatuses(previousChecks: Queue[Boolean]): F[Boolean] =
    checkFreeCapacity.map(previousChecks.enqueue) >>= {
      case checks if checks.size < consecutiveSuccessesToPass => waitAndTryAgain(checks)
      case checks if !checks.forall(identity)                 => waitAndTryAgain(checks.dequeue._2)
      case _                                                  => true.pure[F]
    }

  private def waitAndTryAgain(previousChecks: Queue[Boolean]) =
    Temporal[F].delayBy(waitForSubsequentReadyStatuses(previousChecks), retryTimeout)

  private def checkFreeCapacity =
    elClient.getStatus
      .map(
        _.toEither
          .leftMap(_ => false)
          .map(getTriplesGeneratedFreeCapacity)
          .flatMap(Either.fromOption(_, ifNone = false))
          .map(_ > 0)
          .merge
      )

  private lazy val getTriplesGeneratedFreeCapacity: ServiceStatus => Option[Int] =
    _.subscriptions
      .find(_.name == triplesgenerated.categoryName)
      .flatMap(_.maybeCapacity)
      .map(_.free)
}
