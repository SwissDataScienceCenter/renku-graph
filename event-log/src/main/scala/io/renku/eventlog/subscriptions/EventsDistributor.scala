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

import cats.effect.{ContextShift, Effect, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.circe.Encoder
import io.renku.eventlog.statuschange.commands._
import io.renku.eventlog.statuschange.{IOUpdateCommandsRunner, StatusUpdatesRunner}
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.eventlog.{EventLogDB, EventMessage}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

private trait EventsDistributor[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class EventsDistributorImpl[Interpretation[_]: Effect, FoundEvent](
    subscribers:      Subscribers[Interpretation],
    eventsFinder:     EventFinder[Interpretation, FoundEvent],
    eventsSender:     EventsSender[Interpretation, FoundEvent],
    dispatchRecovery: DispatchRecovery[Interpretation, FoundEvent],
    logger:           Logger[Interpretation],
    noEventSleep:     FiniteDuration,
    onErrorSleep:     FiniteDuration
)(implicit timer:     Timer[Interpretation])
    extends EventsDistributor[Interpretation] {

  import eventsSender._
  import io.renku.eventlog.subscriptions.EventsSender.SendingResult._
  import subscribers._

  def run(): Interpretation[Unit] = for {
    _ <- popEvent flatMap { foundEvent =>
           runOnSubscriber(dispatch(foundEvent)) recoverWith tryReDispatch(foundEvent)
         }
    _ <- run()
  } yield ()

  private def popEvent: Interpretation[FoundEvent] =
    eventsFinder
      .popEvent()
      .recoverWith(loggingErrorAndRetry(retry = eventsFinder.popEvent))
      .flatMap {
        case None            => (timer sleep noEventSleep) flatMap (_ => popEvent)
        case Some(idAndBody) => idAndBody.pure[Interpretation]
      }

  private def dispatch(foundEvent: FoundEvent)(subscriber: SubscriberUrl): Interpretation[Unit] = {
    for {
      result <- sendEvent(subscriber, foundEvent)
      _      <- logStatement(result, subscriber, foundEvent)
      _ <- result match {
             case Delivered => ().pure[Interpretation]
             case ServiceBusy =>
               markBusy(subscriber) recover withNothing flatMap (_ => runOnSubscriber(dispatch(foundEvent)))
             case Misdelivered =>
               delete(subscriber) recover withNothing flatMap (_ => runOnSubscriber(dispatch(foundEvent)))
           }
    } yield ()
  } recoverWith dispatchRecovery.recover(subscriber, foundEvent)

  private def tryReDispatch(foundEvent: FoundEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      for {
        _ <- logger.error(exception)("Dispatching an event failed")
        _ <- timer sleep onErrorSleep
        _ <- runOnSubscriber(dispatch(foundEvent))
      } yield ()
  }

  private def logStatement(result: SendingResult, url: SubscriberUrl, event: FoundEvent): Interpretation[Unit] =
    result match {
      case result @ Delivered    => logger.info(s"Event $event, url = $url -> $result")
      case ServiceBusy           => ().pure[Interpretation]
      case result @ Misdelivered => logger.error(s"Event $event, url = $url -> $result")
    }

  private lazy val withNothing: PartialFunction[Throwable, Unit] = { case NonFatal(_) => () }

  private def loggingErrorAndRetry[O](retry: () => Interpretation[O]): PartialFunction[Throwable, Interpretation[O]] = {
    case NonFatal(exception) =>
      for {
        _      <- logger.error(exception)("Finding events to dispatch failed")
        _      <- timer sleep onErrorSleep
        result <- retry() recoverWith loggingErrorAndRetry(retry)
      } yield result
  }

}

private object IOEventsDistributor {
  private val NoEventSleep: FiniteDuration = 1 seconds
  private val OnErrorSleep: FiniteDuration = 1 seconds

  def apply[FoundEvent](
      transactor:        DbTransactor[IO, EventLogDB],
      subscribers:       Subscribers[IO],
      eventsFinder:      EventFinder[IO, FoundEvent],
      foundEventEncoder: Encoder[FoundEvent],
      dispatchRecovery:  DispatchRecovery[IO, FoundEvent],
      logger:            Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventsDistributor[IO]] =
    for {
      eventsSender <- IOEventsSender[FoundEvent](foundEventEncoder, logger)
    } yield new EventsDistributorImpl(subscribers,
                                      eventsFinder,
                                      eventsSender,
                                      dispatchRecovery,
                                      logger,
                                      noEventSleep = NoEventSleep,
                                      onErrorSleep = OnErrorSleep
    )
}

private trait DispatchRecovery[Interpretation[_], FoundEvent] {
  def recover(
      url:        SubscriberUrl,
      foundEvent: FoundEvent
  ): PartialFunction[Throwable, Interpretation[Unit]]
}
