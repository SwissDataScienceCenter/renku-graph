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
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.events.consumers._
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: Async: Logger: MetricsRegistry: QueriesExecutionTimes](
    override val categoryName: CategoryName,
    childHandlers:             List[consumers.EventHandler[F]] = Nil
) extends consumers.EventHandler[F] {

  override def tryHandling(request: EventRequestContent): F[EventSchedulingResult] =
    checkCategory(request)
      .map { _ =>
        childHandlers.foldLeft(UnsupportedEventType.widen.pure[F]) { case (previousHandlingResult, nextHandler) =>
          previousHandlingResult >>= {
            case r @ Accepted                      => r.widen.pure[F]
            case BadRequest | UnsupportedEventType => nextHandler.tryHandling(request)
            case r                                 => r.pure[F]
          }
        }
      }
      .sequence
      .map(_.merge)
}

private object EventHandler {

  def apply[F[
      _
  ]: Async: SessionResource: AccessTokenFinder: Logger: MetricsRegistry: QueriesExecutionTimes: EventStatusGauges](
      eventsQueue: StatusChangeEventsQueue[F]
  ): F[consumers.EventHandler[F]] = for {
    deliveryInfoRemover               <- DeliveryInfoRemover[F]
    statusChanger                     <- StatusChanger[F]
    rollbackToNew                     <- RollbackToNewHandler[F](statusChanger)
    toTriplesGenerated                <- ToTriplesGeneratedHandler[F](deliveryInfoRemover, statusChanger)
    rollbackToTriplesGenerated        <- RollbackToTriplesGeneratedHandler[F](statusChanger)
    toTriplesStoreHandler             <- ToTriplesStoreHandler[F](deliveryInfoRemover, statusChanger)
    toFailureHandler                  <- ToFailureHandler[F](deliveryInfoRemover, statusChanger)
    toAwaitingDeletionHandler         <- ToAwaitingDeletionHandler[F](statusChanger)
    rollbackToAwaitingDeletionHandler <- RollbackToAwaitingDeletionHandler[F](statusChanger)
    redoProjectTransformationHandler  <- RedoProjectTransformationHandler[F](statusChanger, eventsQueue)
    projectEventsToNewHandler         <- ProjectEventsToNewHandler[F](statusChanger, eventsQueue)
    allEventsToNewHandler             <- AllEventsToNewHandler[F](statusChanger)
  } yield new EventHandler[F](
    categoryName,
    childHandlers = List(
      rollbackToNew,
      toTriplesGenerated,
      rollbackToTriplesGenerated,
      toTriplesStoreHandler,
      toFailureHandler,
      toAwaitingDeletionHandler,
      rollbackToAwaitingDeletionHandler,
      redoProjectTransformationHandler,
      projectEventsToNewHandler,
      allEventsToNewHandler
    )
  )
}
