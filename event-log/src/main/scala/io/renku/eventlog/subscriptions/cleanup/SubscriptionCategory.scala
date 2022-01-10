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

package io.renku.eventlog.subscriptions.cleanup

import cats.Parallel
import cats.effect._
import cats.syntax.all._
import io.renku.eventlog.subscriptions._
import io.renku.eventlog.subscriptions
import io.renku.eventlog.subscriptions.cleanup.CleanUpEventEncoder.encodeEvent
import io.renku.graph.model.events.CategoryName
import org.typelevel.log4cats.Logger
import io.renku.metrics.LabeledHistogram
import io.renku.db.SqlStatement

private[subscriptions] object SubscriptionCategory {

  val name: CategoryName = CategoryName("CLEAN_UP")

  def apply[F[_]: Async: Parallel: Logger](
      subscriberTracker: SubscriberTracker[F],
      queriesExecTimes:  LabeledHistogram[F, SqlStatement.Name]
  ): F[subscriptions.SubscriptionCategory[F]] = for {
    subscribers      <- Subscribers(name, subscriberTracker)
    eventDelivery    <- EventDelivery.noOp[F, CleanUpEvent]
    dispatchRecovery <- LoggingDispatchRecovery[F, CleanUpEvent](name)
    eventFinder      <- CleanUpEventFinder(queriesExecTimes)
    eventsDistributor <-
      EventsDistributor(name, subscribers, eventFinder, eventDelivery, EventEncoder(encodeEvent), dispatchRecovery)
    deserializer <-
      SubscriptionRequestDeserializer[F, SubscriptionCategoryPayload](name, SubscriptionCategoryPayload.apply)
  } yield new SubscriptionCategoryImpl[F, SubscriptionCategoryPayload](name,
                                                                       subscribers,
                                                                       eventsDistributor,
                                                                       deserializer
  )
}
