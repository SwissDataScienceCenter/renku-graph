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
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
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
    underProcessingGauge: LabeledGauge[IO, projects.Path],
    logger:               Logger[IO],
    noSubscriptionSleep:  FiniteDuration,
    noEventSleep:         FiniteDuration,
    onErrorSleep:         FiniteDuration
)(implicit timer:         Timer[IO]) {

  import eventsSender._
  import io.renku.eventlog.subscriptions.EventsSender.SendingResult._
  import subscriptions._

  def run: IO[Unit] =
    for {
      _ <- popEventN flatMap {
            case (eventId, eventBody) => runOnFreeSubscriber(dispatchN(eventId, eventBody))
          }
      _ <- run
    } yield ()

  private def popEventN: IO[(CompoundEventId, EventBody)] =
    eventsFinder.popEvent
      .recoverWith(loggingError("Finding events to process failed", retry = eventsFinder.popEvent))
      .flatMap {
        case None            => (timer sleep noEventSleep) flatMap (_ => popEventN)
        case Some(idAndBody) => idAndBody.pure[IO]
      }

  private def dispatchN(id: CompoundEventId, body: EventBody)(subscriber: SubscriberUrl): IO[Unit] = {
    for {
      result <- sendEvent(subscriber, id, body)
      _      <- logStatement(result, subscriber, id)
      _ <- result match {
            case Delivered    => IO.unit
            case ServiceBusy  => markBusy(subscriber) recover withNothing flatMap (_ => dispatch(id, body))
            case Misdelivered => remove(subscriber) recover withNothing flatMap (_ => dispatch(id, body))
          }
    } yield ()
  } recoverWith markEventAsNonRecoverable(subscriber, id)

  def oldrun: IO[Unit] = waitForSubscriptions(andThen = popEvent)

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
    subscriptions.nextFree flatMap {
      case None => waitForSubscriptions(andThen = dispatch(id, body))
      case Some(url) => {
        for {
          result <- sendEvent(url, id, body)
          _      <- logStatement(result, url, id)
          _ <- result match {
                case Delivered    => IO.unit
                case ServiceBusy  => markBusy(url) recover withNothing flatMap (_ => dispatch(id, body))
                case Misdelivered => remove(url) recover withNothing flatMap (_ => dispatch(id, body))
              }
        } yield ()
      } recoverWith markEventAsNonRecoverable(url, id)
    }

  private def logStatement(result: SendingResult, url: SubscriberUrl, id: CompoundEventId): IO[Unit] = result match {
    case result @ Delivered    => logger.info(s"Event $id, url = $url -> $result")
    case ServiceBusy           => IO.unit
    case result @ Misdelivered => logger.error(s"Event $id, url = $url -> $result")
  }

  private lazy val withNothing: PartialFunction[Throwable, Unit] = {
    case NonFatal(_) => ()
  }

  private def markEventAsNonRecoverable(url: SubscriberUrl, id: CompoundEventId): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      val markEventFailed = ToNonRecoverableFailure[IO](id, EventMessage(exception), underProcessingGauge)
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
      queriesExecTimes:        LabeledHistogram[IO, SqlQuery.Name],
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[EventsDispatcher] =
    for {
      eventsFinder        <- IOEventLogFetch(transactor, waitingEventsGauge, underProcessingGauge, queriesExecTimes)
      eventsSender        <- IOEventsSender(logger)
      updateCommandRunner <- IOUpdateCommandsRunner(transactor, queriesExecTimes, logger)
    } yield new EventsDispatcher(subscriptions,
                                 eventsFinder,
                                 updateCommandRunner,
                                 eventsSender,
                                 underProcessingGauge,
                                 logger,
                                 noSubscriptionSleep = NoSubscriptionSleep,
                                 noEventSleep        = NoEventSleep,
                                 onErrorSleep        = OnErrorSleep)
}
