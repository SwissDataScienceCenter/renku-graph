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

package io.renku.eventlog.events.producers
package tsmigrationrequest

import EventsSender.SendingResult
import cats.data.OptionT
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.renku.events.CategoryName
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait MigrationEventsDistributor[F[_]] {
  def run(): F[Unit]
}

private class MigrationEventsDistributorImpl[F[_]: Temporal: Logger](
    categoryName:     CategoryName,
    subscribers:      Subscribers[F, _],
    eventsFinder:     EventFinder[F],
    eventsSender:     EventsSender[F, MigrationRequestEvent],
    dispatchRecovery: DispatchRecovery[F, MigrationRequestEvent],
    noEventSleep:     FiniteDuration,
    onErrorSleep:     FiniteDuration
) extends EventsDistributor[F] {

  import EventsSender.SendingResult._
  import dispatchRecovery._
  import eventsSender._
  import subscribers._

  def run(): F[Unit] = {
    for {
      _ <- ().pure[F]
      _ <- (popEvent semiflatMap dispatch).value
    } yield ()
  }.foreverM[Unit]

  private def popEvent: OptionT[F, MigrationRequestEvent] = OptionT {
    eventsFinder.popEvent() handleErrorWith logError
  }.flatTapNone(Temporal[F] sleep noEventSleep)

  private lazy val dispatch: MigrationRequestEvent => F[Unit] = { case event @ MigrationRequestEvent(url, _) =>
    (sendEvent(url, event) >>= handleResult(event))
      .recoverWith(recover(event.subscriberUrl, event))
  }

  private def handleResult(event: MigrationRequestEvent): SendingResult => F[Unit] = {
    case result @ Delivered =>
      Logger[F].info(show"$categoryName: $event, subscriber = ${event.subscriberUrl} -> $result")
    case result @ TemporarilyUnavailable =>
      returnToQueue(event, result) handleErrorWith logError(event, "returning event to the queue failed")
    case result @ Misdelivered =>
      Logger[F].info(show"$categoryName: $event, subscriber = ${event.subscriberUrl} -> $result") >>
        delete(event.subscriberUrl) handleErrorWith logError(event, "deleting subscriber failed")
  }

  private def logError(event: MigrationRequestEvent, message: String): PartialFunction[Throwable, F[Unit]] =
    Logger[F].error(_)(show"$categoryName: $event -> $message")

  private lazy val logError: Throwable => F[Option[MigrationRequestEvent]] = { exception =>
    for {
      _ <- Logger[F].error(exception)(show"$categoryName: finding events to dispatch failed")
      _ <- Temporal[F] sleep onErrorSleep
    } yield Option.empty[MigrationRequestEvent]
  }
}

private object MigrationEventsDistributor {
  private val NoEventSleep: FiniteDuration = 1 minute
  private val OnErrorSleep: FiniteDuration = 1 minute

  def apply[F[_]: Async: Logger: MetricsRegistry](
      subscribers:          Subscribers[F, _],
      eventsFinder:         EventFinder[F],
      categoryEventEncoder: EventEncoder[MigrationRequestEvent],
      dispatchRecovery:     DispatchRecovery[F, MigrationRequestEvent]
  ): F[EventsDistributor[F]] =
    EventsSender[F, MigrationRequestEvent](categoryName, categoryEventEncoder).map(
      new MigrationEventsDistributorImpl(categoryName,
                                         subscribers,
                                         eventsFinder,
                                         _,
                                         dispatchRecovery,
                                         noEventSleep = NoEventSleep,
                                         onErrorSleep = OnErrorSleep
      )
    )
}
