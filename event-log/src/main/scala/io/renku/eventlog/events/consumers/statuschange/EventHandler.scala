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

package io.renku.eventlog.events.consumers.statuschange

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, DecodingFailure}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes}
import io.renku.events.consumers.{EventSchedulingResult, ProcessExecutor}
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

final class EventHandler[F[_]: Async: Logger: MetricsRegistry: QueriesExecutionTimes](
    processExecutor:     ProcessExecutor[F],
    statusChanger:       StatusChanger[F],
    eventSender:         EventSender[F],
    eventsQueue:         StatusChangeEventsQueue[F],
    deliveryInfoRemover: DeliveryInfoRemover[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  override val categoryName: CategoryName = io.renku.eventlog.events.consumers.statuschange.categoryName

  protected override type Event = StatusChangeEvent

  protected override def onPostHandling(event: StatusChangeEvent, result: EventSchedulingResult): F[Unit] =
    result match {
      case EventSchedulingResult.Accepted =>
        if (logEventAccepted(event)) Logger[F].info(show"$categoryName: $event -> $result")
        else ().pure[F]

      case EventSchedulingResult.SchedulingError(ex) =>
        Logger[F].error(ex)(show"$categoryName: $event -> $result")

      case _ =>
        Logger[F].info(show"$categoryName: $event -> $result")
    }

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = eventDecoder,
      process = statusChanger.updateStatuses(dbUpdater)
    )

  private val dbUpdater: DBUpdater[F, StatusChangeEvent] =
    new DBUpdater[F, StatusChangeEvent] {
      override def updateDB(event: StatusChangeEvent): UpdateOp[F] =
        dbUpdaterFor(event)._1

      override def onRollback(event: StatusChangeEvent): RollbackOp[F] =
        dbUpdaterFor(event)._2
    }

  private val eventDecoder: EventRequestContent => Either[DecodingFailure, StatusChangeEvent] = req =>
    Decoder[StatusChangeEvent]
      .emap {
        case e: StatusChangeEvent.ToTriplesGenerated =>
          req match {
            case EventRequestContent.WithPayload(_, payload: ZippedEventPayload) =>
              e.copy(payload = payload).asRight
            case _ =>
              Left(show"Missing event payload for: $e")
          }
        case e => e.asRight
      }
      .apply(req.event.hcursor)

  private def dbUpdaterFor(event: StatusChangeEvent): (UpdateOp[F], RollbackOp[F]) =
    event match {
      case ev: StatusChangeEvent.AllEventsToNew.type =>
        val updater = new alleventstonew.DbUpdater[F](eventSender)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.ProjectEventsToNew =>
        val updater = new projecteventstonew.DbUpdater[F](eventsQueue)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.RedoProjectTransformation =>
        val updater = new redoprojecttransformation.DbUpdater[F](eventsQueue)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.RollbackToAwaitingDeletion =>
        val updater = new rollbacktoawaitingdeletion.DbUpdater[F]()
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.RollbackToNew =>
        val updater = new rollbacktonew.DbUpdater[F]()
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.RollbackToTriplesGenerated =>
        val updater = new rollbacktotriplesgenerated.DbUpdater[F]()
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.ToAwaitingDeletion =>
        val updater = new toawaitingdeletion.DbUpdater[F]()
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.ToFailure =>
        val updater = new tofailure.DbUpdater[F](deliveryInfoRemover)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.ToTriplesGenerated =>
        val updater = new totriplesgenerated.DbUpdater[F](deliveryInfoRemover)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: StatusChangeEvent.ToTriplesStore =>
        val updater = new totriplesstore.DbUpdater[F](deliveryInfoRemover)
        (updater.updateDB(ev), updater.onRollback(ev))
    }

  private def logEventAccepted(event: StatusChangeEvent) = event match {
    case _: StatusChangeEvent.RollbackToNew              => false
    case _: StatusChangeEvent.RollbackToAwaitingDeletion => false
    case _: StatusChangeEvent.RollbackToTriplesGenerated => false
    case _ => true
  }
}

object EventHandler {

  def apply[F[
      _
  ]: Async: SessionResource: AccessTokenFinder: Logger: MetricsRegistry: QueriesExecutionTimes: EventStatusGauges](
      eventsQueue: StatusChangeEventsQueue[F]
  ): F[consumers.EventHandler[F]] = for {
    deliveryInfoRemover       <- DeliveryInfoRemover[F]
    statusChanger             <- StatusChanger[F]
    redoDequeuedEventHandler  <- redoprojecttransformation.DequeuedEventHandler[F]
    toNewDequeuedEventHandler <- projecteventstonew.DequeuedEventHandler[F]
    _ <- eventsQueue.register(
           redoprojecttransformation.eventType,
           statusChanger.updateStatuses(redoDequeuedEventHandler)(_: StatusChangeEvent.RedoProjectTransformation)
         )
    _ <- eventsQueue.register(
           projecteventstonew.eventType,
           statusChanger.updateStatuses(toNewDequeuedEventHandler)(_: StatusChangeEvent.ProjectEventsToNew)
         )
    eventSender     <- EventSender[F](EventLogUrl)
    processExecutor <- ProcessExecutor.concurrent(200)
  } yield new EventHandler[F](
    processExecutor,
    statusChanger,
    eventSender,
    eventsQueue,
    deliveryInfoRemover
  )
}
