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

package io.renku.eventlog.subscriptions

import cats.data.OptionT
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.eventlog.subscriptions.eventdelivery.EventDelivery
import io.renku.events.CategoryName
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.util.control.NonFatal

private trait EventsDistributor[F[_]] {
  def run(): F[Unit]
}

private class EventsDistributorImpl[F[_]: MonadThrow: Temporal: Logger, CategoryEvent](
    categoryName:                 CategoryName,
    subscribers:                  Subscribers[F, _],
    eventsFinder:                 EventFinder[F, CategoryEvent],
    eventsSender:                 EventsSender[F, CategoryEvent],
    eventDelivery:                EventDelivery[F, CategoryEvent],
    dispatchRecovery:             DispatchRecovery[F, CategoryEvent],
    noEventSleep:                 FiniteDuration,
    onErrorSleep:                 FiniteDuration
)(implicit val showCategoryEvent: Show[CategoryEvent])
    extends EventsDistributor[F] {

  import dispatchRecovery._
  import eventsSender._
  import io.renku.eventlog.subscriptions.EventsSender.SendingResult._
  import subscribers._

  def run(): F[Unit] = {
    for {
      _ <- ().pure[F]
      _ <- runOnSubscriber(popAndDispatch) recoverWith logAndWait
    } yield ()
  }.foreverM[Unit]

  private lazy val popAndDispatch: SubscriberUrl => F[Unit] =
    subscriber => (popEvent semiflatMap dispatch(subscriber)).value.void

  private def popEvent: OptionT[F, CategoryEvent] = OptionT {
    eventsFinder.popEvent() recoverWith logError
  }.flatTapNone(Temporal[F] sleep noEventSleep)

  private def dispatch(
      subscriber: SubscriberUrl
  )(event:        CategoryEvent): F[Unit] = {
    sendEvent(subscriber, event) >>= handleResult(subscriber, event)
  } recoverWith recover(subscriber, event)

  private lazy val logAndWait: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Temporal[F].andWait(
      Logger[F].error(exception)(show"$categoryName: executing event distribution on a subscriber failed"),
      onErrorSleep
    )
  }

  private def handleResult(subscriber: SubscriberUrl, event: CategoryEvent): SendingResult => F[Unit] = {
    case result @ Delivered =>
      Logger[F].info(show"$categoryName: $event, subscriber = $subscriber -> $result")
      eventDelivery.registerSending(event, subscriber) recoverWith logError(event, subscriber)
    case result @ TemporarilyUnavailable =>
      (markBusy(subscriber) recover withNothing) >> (returnToQueue(event, result) recoverWith logError(event))
    case result @ Misdelivered =>
      Logger[F].error(show"$categoryName: $event, subscriber = $subscriber -> $result")
      (delete(subscriber) recover withNothing) >> (returnToQueue(event, result) recoverWith logError(event))
  }

  private lazy val withNothing: PartialFunction[Throwable, Unit] = { case NonFatal(_) => () }

  private def logError(event: CategoryEvent, subscriberUrl: SubscriberUrl): PartialFunction[Throwable, F[Unit]] = {
    case NonFatal(exception) =>
      Logger[F].error(exception)(s"$categoryName: registering sending $event to $subscriberUrl failed")
  }

  private def logError(event: CategoryEvent): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"$categoryName: $event -> returning an event to the queue failed")
  }

  private lazy val logError: PartialFunction[Throwable, F[Option[CategoryEvent]]] = { case NonFatal(exception) =>
    for {
      _ <- Logger[F].error(exception)(s"$categoryName: finding events to dispatch failed")
      _ <- Temporal[F] sleep onErrorSleep
    } yield Option.empty[CategoryEvent]
  }
}

private object EventsDistributor {
  private val NoEventSleep: FiniteDuration = 1 seconds
  private val OnErrorSleep: FiniteDuration = 1 seconds

  def apply[F[_]: Async: Logger: MetricsRegistry, CategoryEvent](
      categoryName:             CategoryName,
      subscribers:              Subscribers[F, _],
      eventsFinder:             EventFinder[F, CategoryEvent],
      eventDelivery:            EventDelivery[F, CategoryEvent],
      categoryEventEncoder:     EventEncoder[CategoryEvent],
      dispatchRecovery:         DispatchRecovery[F, CategoryEvent]
  )(implicit showCategoryEvent: Show[CategoryEvent]): F[EventsDistributor[F]] = for {
    eventsSender <- EventsSender[F, CategoryEvent](categoryName, categoryEventEncoder)
  } yield new EventsDistributorImpl(categoryName,
                                    subscribers,
                                    eventsFinder,
                                    eventsSender,
                                    eventDelivery,
                                    dispatchRecovery,
                                    noEventSleep = NoEventSleep,
                                    onErrorSleep = OnErrorSleep
  )
}
