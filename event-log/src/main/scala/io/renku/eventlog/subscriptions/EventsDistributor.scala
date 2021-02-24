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

import cats.MonadError
import cats.effect.{ContextShift, Effect, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.DbTransactor
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.CategoryName
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.EventsSender.SendingResult

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

private trait EventsDistributor[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class EventsDistributorImpl[Interpretation[_]: Effect, CategoryEvent](
    categoryName:     CategoryName,
    subscribers:      Subscribers[Interpretation],
    eventsFinder:     EventFinder[Interpretation, CategoryEvent],
    eventsSender:     EventsSender[Interpretation, CategoryEvent],
    dispatchRecovery: DispatchRecovery[Interpretation, CategoryEvent],
    logger:           Logger[Interpretation],
    noEventSleep:     FiniteDuration,
    onErrorSleep:     FiniteDuration
)(implicit timer:     Timer[Interpretation], ME: MonadError[Interpretation, Throwable])
    extends EventsDistributor[Interpretation] {

  import eventsSender._
  import io.renku.eventlog.subscriptions.EventsSender.SendingResult._
  import subscribers._

  def run(): Interpretation[Unit] = {
    for {
      _ <- ME.unit
      _ <- popEvent flatMap { categoryEvent =>
             runOnSubscriber(dispatch(categoryEvent)) recoverWith tryReDispatch(categoryEvent)
           }
    } yield ()
  }.foreverM[Unit]

  private def popEvent: Interpretation[CategoryEvent] =
    eventsFinder
      .popEvent()
      .recoverWith(loggingErrorAndRetry(retry = () => eventsFinder.popEvent()))
      .flatMap {
        case None            => (timer sleep noEventSleep) flatMap (_ => popEvent)
        case Some(idAndBody) => idAndBody.pure[Interpretation]
      }

  private def dispatch(categoryEvent: CategoryEvent)(subscriber: SubscriberUrl): Interpretation[Unit] = {
    for {
      result <- sendEvent(subscriber, categoryEvent)
      _      <- logStatement(result, subscriber, categoryEvent)
      _ <- result match {
             case Delivered => ().pure[Interpretation]
             case ServiceBusy =>
               markBusy(subscriber) recover withNothing flatMap (_ => runOnSubscriber(dispatch(categoryEvent)))
             case Misdelivered =>
               delete(subscriber) recover withNothing flatMap (_ => runOnSubscriber(dispatch(categoryEvent)))
           }
    } yield ()
  } recoverWith dispatchRecovery.recover(subscriber, categoryEvent)

  private def tryReDispatch(categoryEvent: CategoryEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      for {
        _ <- logger.error(exception)(s"$categoryName: Dispatching an event failed")
        _ <- timer sleep onErrorSleep
        _ <- runOnSubscriber(dispatch(categoryEvent))
      } yield ()
  }

  private def logStatement(result: SendingResult, url: SubscriberUrl, event: CategoryEvent): Interpretation[Unit] =
    result match {
      case result @ Delivered    => logger.info(s"$categoryName: $event, url = $url -> $result")
      case ServiceBusy           => ().pure[Interpretation]
      case result @ Misdelivered => logger.error(s"$categoryName: $event, url = $url -> $result")
    }

  private lazy val withNothing: PartialFunction[Throwable, Unit] = { case NonFatal(_) => () }

  private def loggingErrorAndRetry[O](retry: () => Interpretation[O]): PartialFunction[Throwable, Interpretation[O]] = {
    case NonFatal(exception) =>
      for {
        _      <- logger.error(exception)(s"$categoryName: Finding events to dispatch failed")
        _      <- timer sleep onErrorSleep
        result <- retry() recoverWith loggingErrorAndRetry(retry)
      } yield result
  }
}

private object IOEventsDistributor {
  private val NoEventSleep: FiniteDuration = 1 seconds
  private val OnErrorSleep: FiniteDuration = 1 seconds

  def apply[CategoryEvent](
      categoryName:         CategoryName,
      transactor:           DbTransactor[IO, EventLogDB],
      subscribers:          Subscribers[IO],
      eventsFinder:         EventFinder[IO, CategoryEvent],
      categoryEventEncoder: EventEncoder[CategoryEvent],
      dispatchRecovery:     DispatchRecovery[IO, CategoryEvent],
      logger:               Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventsDistributor[IO]] =
    for {
      eventsSender <- IOEventsSender[CategoryEvent](categoryEventEncoder, logger)
    } yield new EventsDistributorImpl(categoryName,
                                      subscribers,
                                      eventsFinder,
                                      eventsSender,
                                      dispatchRecovery,
                                      logger,
                                      noEventSleep = NoEventSleep,
                                      onErrorSleep = OnErrorSleep
    )
}
