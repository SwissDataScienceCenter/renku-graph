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
import io.renku.events.{consumers, CategoryName, EventRequestContent}
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
    deliveryInfoRemover <- DeliveryInfoRemover[F]
    statusChanger       <- StatusChanger[F]
    childHandlers <- List(
                       rollbacktonew.Handler[F](statusChanger),
                       totriplesgenerated.Handler[F](deliveryInfoRemover, statusChanger),
                       rollbacktotriplesgenerated.Handler[F](statusChanger),
                       totriplesstore.Handler[F](deliveryInfoRemover, statusChanger),
                       tofailure.Handler[F](deliveryInfoRemover, statusChanger),
                       toawaitingdeletion.Handler[F](statusChanger),
                       rollbacktoawaitingdeletion.Handler[F](statusChanger),
                       redoprojecttransformation.Handler[F](statusChanger, eventsQueue),
                       projecteventstonew.Handler[F](statusChanger, eventsQueue),
                       alleventstonew.Handler[F](statusChanger)
                     ).sequence
  } yield new EventHandler[F](categoryName, childHandlers)
}
