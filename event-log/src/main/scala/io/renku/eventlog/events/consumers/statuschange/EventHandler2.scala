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
import io.circe.{Decoder, DecodingFailure}
import io.renku.eventlog.events.consumers.statuschange.alleventstonew.AllEventsToNew
import io.renku.eventlog.events.consumers.statuschange.projecteventstonew.ProjectEventsToNew
import io.renku.eventlog.events.consumers.statuschange.redoprojecttransformation.RedoProjectTransformation
import io.renku.eventlog.events.consumers.statuschange.rollbacktoawaitingdeletion.RollbackToAwaitingDeletion
import io.renku.eventlog.events.consumers.statuschange.rollbacktonew.RollbackToNew
import io.renku.eventlog.events.consumers.statuschange.rollbacktotriplesgenerated.RollbackToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange.toawaitingdeletion.ToAwaitingDeletion
import io.renku.eventlog.events.consumers.statuschange.tofailure.ToFailure
import io.renku.eventlog.events.consumers.statuschange.totriplesgenerated.ToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange.totriplesstore.ToTriplesStore
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

class EventHandler2[F[_]: Async: Logger: MetricsRegistry: QueriesExecutionTimes](
    processExecutor:     ProcessExecutor[F],
    statusChanger:       StatusChanger[F],
    eventSender:         EventSender[F],
    eventsQueue:         StatusChangeEventsQueue[F],
    deliveryInfoRemover: DeliveryInfoRemover[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  override val categoryName: CategoryName = io.renku.eventlog.events.consumers.statuschange.categoryName

  protected override type Event = StatusChangeEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = eventDecoder,
      process = statusChanger.updateStatuses(dbUpdater)
    )

  private val dbUpdater: DBUpdater[F, StatusChangeEvent] =
    new DBUpdater[F, StatusChangeEvent] {
      override def updateDB(event: StatusChangeEvent): UpdateResult[F] =
        dbUpdaterFor(event)._1

      override def onRollback(event: StatusChangeEvent): RollbackResult[F] =
        dbUpdaterFor(event)._2
    }

  // note that order matters, the first decoder succeeding will win
  private val subEventDecoders: List[EventRequestContent => Either[DecodingFailure, StatusChangeEvent]] = List(
    RollbackToNew.decoder,
    ToTriplesGenerated.decoder,
    RollbackToTriplesGenerated.decoder,
    ToTriplesStore.decoder,
    ToFailure.decoder,
    ToAwaitingDeletion.decoder,
    RollbackToAwaitingDeletion.decoder,
    RedoProjectTransformation.decoder,
    ProjectEventsToNew.decoder,
    AllEventsToNew.decoder
  )

  private val eventDecoder: EventRequestContent => Either[DecodingFailure, StatusChangeEvent] = req =>
    Decoder[RawStatusChangeEvent]
      .emap {
        case RollbackToNew(e) => Right(e)
        case ToTriplesGenerated(f) =>
          req match {
            case EventRequestContent.WithPayload(_, payload: ZippedEventPayload) =>
              Right(f(payload))
            case _ => Left("Missing event payload")
          }
        case RollbackToTriplesGenerated(e) => Right(e)
        case ToTriplesStore(e)             => Right(e)
        case ToFailure(e)                  => Right(e)
        case ToAwaitingDeletion(e)         => Right(e)
        case RollbackToAwaitingDeletion(e) => Right(e)
        case RedoProjectTransformation(e)  => Right(e)
        case ProjectEventsToNew(e)         => Right(e)
        case AllEventsToNew(e)             => Right(e)
        case _                             => Left("Cannot read event")
      }
      .apply(req.event.hcursor)

  private def dbUpdaterFor(event: StatusChangeEvent): (UpdateResult[F], RollbackResult[F]) =
    event match {
      case ev: AllEventsToNew =>
        val updater = new alleventstonew.DbUpdater[F](eventSender)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: ProjectEventsToNew =>
        val updater = new projecteventstonew.DbUpdater[F](eventsQueue)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: RedoProjectTransformation =>
        val updater = new redoprojecttransformation.DbUpdater[F](eventsQueue)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: RollbackToAwaitingDeletion =>
        val updater = new rollbacktoawaitingdeletion.DbUpdater[F]()
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: RollbackToNew =>
        val updater = new rollbacktonew.DbUpdater[F]()
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: RollbackToTriplesGenerated =>
        val updater = new rollbacktotriplesgenerated.DbUpdater[F]()
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: ToAwaitingDeletion =>
        val updater = new toawaitingdeletion.DbUpdater[F]()
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: ToFailure[_, _] =>
        val updater = new tofailure.DbUpdater[F](deliveryInfoRemover)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: ToTriplesGenerated =>
        val updater = new totriplesgenerated.DbUpdater[F](deliveryInfoRemover)
        (updater.updateDB(ev), updater.onRollback(ev))

      case ev: ToTriplesStore =>
        val updater = new totriplesstore.DbUpdater[F](deliveryInfoRemover)
        (updater.updateDB(ev), updater.onRollback(ev))

      // the list must be kept in sync wth subEventDecoders, one slight disadvantage
      // could make a sealed trait in theory
    }

}
