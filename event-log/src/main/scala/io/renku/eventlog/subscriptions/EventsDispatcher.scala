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
import io.renku.eventlog.{EventLogDB, EventMessage}
import io.renku.eventlog.statuschange.{IOUpdateCommandsRunner, StatusUpdatesRunner}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, ToNonRecoverableFailure, UpdateResult}
import io.renku.eventlog.subscriptions.EventsSender.SendingResult

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}
import scala.util.Random.shuffle
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

  import eventsFinder._
  import eventsSender._
  import io.renku.eventlog.subscriptions.EventsSender.SendingResult._

  def run: IO[Unit] =
    subscriptions.getAll flatMap {
      case Nil =>
        logger.info("Waiting for subscribers")
        (timer sleep noSubscriptionSleep) flatMap (_ => run)
      case urls =>
        popEvent flatMap {
          case None             => (timer sleep noEventSleep) flatMap (_ => run)
          case Some((id, body)) => dispatch(shuffle(urls), id, body) flatMap (_ => run)
        } recoverWith loggingError("Finding events to process failed")
    } recoverWith loggingError("Finding subscribers failed")

  private def dispatch(urls: List[SubscriptionUrl], id: CompoundEventId, body: EventBody): IO[Unit] = urls match {
    case url +: otherUrls => {
      for {
        result <- sendEvent(url, id, body)
        _      <- logStatement(result, url, id)
        _ <- result match {
              case Delivered   => IO.unit
              case ServiceBusy => dispatch(otherUrls :+ url, id, body)
              case Misdelivered =>
                subscriptions
                  .remove(url)
                  .flatMap(_ => dispatch(otherUrls, id, body))
                  .recoverWith(redispatch(otherUrls, id, body))
            }
      } yield ()
    } recoverWith nonRecoverableError(url, id)
    case Nil => run
  }

  private def logStatement(result: SendingResult, url: SubscriptionUrl, id: CompoundEventId): IO[Unit] = result match {
    case result @ Delivered    => logger.info(s"Event $id, url = $url -> $result")
    case result @ ServiceBusy  => logger.info(s"Event $id, url = $url -> $result")
    case result @ Misdelivered => logger.error(s"Event $id, url = $url -> $result")
  }

  private def nonRecoverableError(url: SubscriptionUrl, id: CompoundEventId): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      val markEventFailed =
        ToNonRecoverableFailure[IO](id, EventMessage(exception), waitingEventsGauge, underProcessingGauge)
      for {
        _ <- statusUpdatesRunner run markEventFailed recoverWith retry(markEventFailed)
        _ <- logger.error(exception)(s"Event $id, url = $url -> ${markEventFailed.status}")
      } yield ()
  }

  private def loggingError(message: String): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(message)
      (timer sleep onErrorSleep) flatMap (_ => run)
  }

  private def redispatch(urls: List[SubscriptionUrl],
                         id:   CompoundEventId,
                         body: EventBody): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Removing subscription failed")
      dispatch(urls, id, body)
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
      updateCommandRunner <- IOUpdateCommandsRunner(transactor)
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
