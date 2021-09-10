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

package io.renku.eventlog.subscriptions

import cats.{MonadThrow, Show}
import cats.data.OptionT
import cats.effect.{ContextShift, Effect, IO, Timer}
import cats.syntax.all._
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.CategoryName
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

private trait EventsDistributor[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class EventsDistributorImpl[Interpretation[_]: Effect: MonadThrow: Timer, CategoryEvent](
    categoryName:                 CategoryName,
    subscribers:                  Subscribers[Interpretation],
    eventsFinder:                 EventFinder[Interpretation, CategoryEvent],
    eventsSender:                 EventsSender[Interpretation, CategoryEvent],
    eventDelivery:                EventDelivery[Interpretation, CategoryEvent],
    dispatchRecovery:             DispatchRecovery[Interpretation, CategoryEvent],
    logger:                       Logger[Interpretation],
    noEventSleep:                 FiniteDuration,
    onErrorSleep:                 FiniteDuration
)(implicit val showCategoryEvent: Show[CategoryEvent])
    extends EventsDistributor[Interpretation] {

  import dispatchRecovery._
  import eventsSender._
  import io.renku.eventlog.subscriptions.EventsSender.SendingResult._
  import subscribers._

  def run(): Interpretation[Unit] = {
    for {
      _ <- ().pure[Interpretation]
      _ <- runOnSubscriber(popAndDispatch) recoverWith logAndWait
    } yield ()
  }.foreverM[Unit]

  private lazy val popAndDispatch: SubscriberUrl => Interpretation[Unit] =
    subscriber => (popEvent semiflatMap dispatch(subscriber)).value.void

  private def popEvent: OptionT[Interpretation, CategoryEvent] = OptionT {
    eventsFinder.popEvent() recoverWith logError
  }.flatTapNone(Timer[Interpretation] sleep noEventSleep)

  private def dispatch(
      subscriber: SubscriberUrl
  )(event:        CategoryEvent): Interpretation[Unit] = {
    sendEvent(subscriber, event) >>= handleResult(subscriber, event)
  } recoverWith recover(subscriber, event)

  private lazy val logAndWait: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    for {
      _ <- logger.error(exception)(show"$categoryName: executing event distribution on a subscriber failed")
      _ <- Timer[Interpretation] sleep onErrorSleep
    } yield ()
  }

  private def handleResult(subscriber: SubscriberUrl, event: CategoryEvent): SendingResult => Interpretation[Unit] = {
    case result @ Delivered =>
      logger.info(show"$categoryName: $event, subscriber = $subscriber -> $result")
      eventDelivery.registerSending(event, subscriber) recoverWith logError(event, subscriber)
    case TemporarilyUnavailable =>
      (markBusy(subscriber) recover withNothing) >> (returnToQueue(event) recoverWith logError(event))
    case result @ Misdelivered =>
      logger.error(show"$categoryName: $event, subscriber = $subscriber -> $result")
      (delete(subscriber) recover withNothing) >> (returnToQueue(event) recoverWith logError(event))
  }

  private lazy val withNothing: PartialFunction[Throwable, Unit] = { case NonFatal(_) => () }

  private def logError(event:         CategoryEvent,
                       subscriberUrl: SubscriberUrl
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"$categoryName: registering sending $event to $subscriberUrl failed")
  }

  private def logError(event: CategoryEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"$categoryName: $event -> returning an event to the queue failed")
  }

  private lazy val logError: PartialFunction[Throwable, Interpretation[Option[CategoryEvent]]] = {
    case NonFatal(exception) =>
      for {
        _ <- logger.error(exception)(s"$categoryName: finding events to dispatch failed")
        _ <- Timer[Interpretation] sleep onErrorSleep
      } yield Option.empty[CategoryEvent]
  }
}

private object IOEventsDistributor {
  private val NoEventSleep: FiniteDuration = 1 seconds
  private val OnErrorSleep: FiniteDuration = 1 seconds

  def apply[CategoryEvent](
      categoryName:         CategoryName,
      subscribers:          Subscribers[IO],
      eventsFinder:         EventFinder[IO, CategoryEvent],
      eventDelivery:        EventDelivery[IO, CategoryEvent],
      categoryEventEncoder: EventEncoder[CategoryEvent],
      dispatchRecovery:     DispatchRecovery[IO, CategoryEvent],
      logger:               Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      show:             Show[CategoryEvent]
  ): IO[EventsDistributor[IO]] =
    for {
      eventsSender <- IOEventsSender[CategoryEvent](categoryName, categoryEventEncoder, logger)
    } yield new EventsDistributorImpl(categoryName,
                                      subscribers,
                                      eventsFinder,
                                      eventsSender,
                                      eventDelivery,
                                      dispatchRecovery,
                                      logger,
                                      noEventSleep = NoEventSleep,
                                      onErrorSleep = OnErrorSleep
    )
}
