/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers
package cleanup

import CleanUpEventEncoder.encodeEvent
import cats.Parallel
import cats.effect._
import cats.syntax.all._
import eventdelivery._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.events.CategoryName
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram, MetricsRegistry}
import org.typelevel.log4cats.Logger

private[producers] object SubscriptionCategory {

  val name: CategoryName = CategoryName("CLEAN_UP")

  def apply[F[_]: Async: Parallel: SessionResource: UrlAndIdSubscriberTracker: Logger: MetricsRegistry](
      awaitingDeletionGauge: LabeledGauge[F, projects.Path],
      deletingGauge:         LabeledGauge[F, projects.Path],
      queriesExecTimes:      LabeledHistogram[F]
  ): F[SubscriptionCategory[F]] = for {
    subscribers <- UrlAndIdSubscribers[F](name)
    eventDelivery <- eventdelivery.EventDelivery[F, CleanUpEvent](
                       eventDeliveryIdExtractor = (event: CleanUpEvent) => DeletingProjectDeliverId(event.project.id),
                       queriesExecTimes
                     )
    dispatchRecovery <- DispatchRecovery[F]
    eventFinder      <- EventFinder(awaitingDeletionGauge, deletingGauge, queriesExecTimes)
    eventsDistributor <-
      EventsDistributor(name, subscribers, eventFinder, eventDelivery, EventEncoder(encodeEvent), dispatchRecovery)
    deserializer <- UrlAndIdSubscriptionDeserializer[F, SubscriptionPayload](name, SubscriptionPayload.apply)
  } yield new SubscriptionCategoryImpl[F, SubscriptionPayload](name, subscribers, eventsDistributor, deserializer)
}
