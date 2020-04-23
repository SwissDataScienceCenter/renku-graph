/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.statuschange.commands._
import io.renku.eventlog.statuschange.{IOUpdateCommandsRunner, StatusUpdatesRunner}
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.eventlog.{EventLogDB, EventMessage}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

class EventsDispatcher(
    subscriptions:        Subscriptions[IO],
    eventsFinder:         EventFetcher[IO],
    statusUpdatesRunner:  StatusUpdatesRunner[IO],
    eventsSender:         EventsSender[IO],
    waitingEventsGauge:   LabeledGauge[IO, projects.Path],
    underProcessingGauge: LabeledGauge[IO, projects.Path],
    logger:               Logger[IO],
    noSubscriptionSleep:  FiniteDuration,
    noEventSleep:         FiniteDuration,
    onErrorSleep:         FiniteDuration
)(implicit timer:         Timer[IO]) {

  import eventsSender._
  import io.renku.eventlog.subscriptions.EventsSender.SendingResult._

  def run: IO[Unit] = waitForSubscriptions(andThen = popEvent)

  private def waitForSubscriptions(andThen: => IO[Unit]): IO[Unit] = {
    subscriptions.isNext flatMap {
      case true => andThen
      case false =>
        for {
          _ <- logger.info("Waiting for subscribers")
          _ <- timer sleep noSubscriptionSleep
          _ <- waitForSubscriptions(andThen)
        } yield ()
    }
  } recoverWith loggingError("Finding subscribers failed", retry = waitForSubscriptions(andThen))

  private def popEvent: IO[Unit] =
    eventsFinder.popEvent
      .recoverWith(loggingError("Finding events to process failed", retry = eventsFinder.popEvent))
      .flatMap {
        case None             => (timer sleep noEventSleep) flatMap (_ => popEvent)
        case Some((id, body)) => dispatch(id, body) flatMap (_ => popEvent)
      }

  private def dispatch(id: CompoundEventId, body: EventBody): IO[Unit] =
    subscriptions.next flatMap {
      case Some(url) => {
        for {
          result <- sendEvent(url, id, body)
          _      <- logStatement(result, url, id)
          _ <- result match {
                case Delivered    => IO.unit
                case ServiceBusy  => reDispatch(id, body, url)
                case Misdelivered => removeUrlAndRollback(id, body, url)
              }
        } yield ()
      } recoverWith markEventAsRecoverable(url, id)
      case None => waitForSubscriptions(andThen = dispatch(id, body))
    }

  private def toNew(id: CompoundEventId) = ToNew[IO](id, waitingEventsGauge, underProcessingGauge)

  private def logStatement(result: SendingResult, url: SubscriptionUrl, id: CompoundEventId): IO[Unit] = result match {
    case result @ Delivered    => logger.info(s"Event $id, url = $url -> $result")
    case result @ ServiceBusy  => IO.unit
    case result @ Misdelivered => logger.error(s"Event $id, url = $url -> $result")
  }

  private def reDispatch(id: CompoundEventId, body: EventBody, url: SubscriptionUrl) =
    (subscriptions hasOtherThan url) flatMap {
      case true  => dispatch(id, body)
      case false => (timer sleep noSubscriptionSleep) flatMap (_ => dispatch(id, body))
    }

  private def removeUrlAndRollback(id: CompoundEventId, body: EventBody, url: SubscriptionUrl) = {
    for {
      _ <- subscriptions remove url
      _ <- subscriptions.isNext flatMap {
            case true => dispatch(id, body)
            case false =>
              statusUpdatesRunner run toNew(id) map (_ => ()) recoverWith retryDispatching(
                id,
                body,
                s"Event $id problems to rollback event"
              )
          }
    } yield ()
  } recoverWith retryDispatching(id, body, "Removing subscription failed")

  private def retryDispatching(id:      CompoundEventId,
                               body:    EventBody,
                               message: String): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      for {
        _ <- logger.error(exception)(message)
        _ <- dispatch(id, body)
      } yield ()
  }

  private def markEventAsRecoverable(url: SubscriptionUrl, id: CompoundEventId): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      val markEventFailed = ToNonRecoverableFailure[IO](
        id,
        EventMessage(exception),
        waitingEventsGauge,
        underProcessingGauge
      )
      for {
        _ <- statusUpdatesRunner run markEventFailed recoverWith retry(markEventFailed)
        _ <- logger.error(exception)(s"Event $id, url = $url -> ${markEventFailed.status}")
      } yield ()
  }

  private def loggingError[O](message: String, retry: => IO[O]): PartialFunction[Throwable, IO[O]] = {
    case NonFatal(exception) =>
      for {
        _      <- logger.error(exception)(message)
        _      <- timer sleep onErrorSleep
        result <- retry
      } yield result
  }

  private def retry(command: ChangeStatusCommand[IO]): PartialFunction[Throwable, IO[UpdateResult]] = {
    case NonFatal(exception) => {
      for {
        _      <- logger.error(exception)(s"Marking event as ${command.status} failed")
        _      <- timer sleep onErrorSleep
        result <- statusUpdatesRunner run command
      } yield result
    } recoverWith retry(command)
  }
}

object EventsDispatcher {
  private val NoSubscriptionSleep: FiniteDuration = 2 seconds
  private val NoEventSleep:        FiniteDuration = 1 seconds
  private val OnErrorSleep:        FiniteDuration = 1 seconds

  def apply(
      transactor:              DbTransactor[IO, EventLogDB],
      subscriptions:           Subscriptions[IO],
      waitingEventsGauge:      LabeledGauge[IO, projects.Path],
      underProcessingGauge:    LabeledGauge[IO, projects.Path],
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[EventsDispatcher] =
    for {
      eventsFinder        <- IOEventLogFetch(transactor, waitingEventsGauge, underProcessingGauge)
      eventsSender        <- IOEventsSender(logger)
      updateCommandRunner <- IOUpdateCommandsRunner(transactor, logger)
    } yield new EventsDispatcher(subscriptions,
                                 eventsFinder,
                                 updateCommandRunner,
                                 eventsSender,
                                 waitingEventsGauge,
                                 underProcessingGauge,
                                 logger,
                                 noSubscriptionSleep = NoSubscriptionSleep,
                                 noEventSleep        = NoEventSleep,
                                 onErrorSleep        = OnErrorSleep)
}
