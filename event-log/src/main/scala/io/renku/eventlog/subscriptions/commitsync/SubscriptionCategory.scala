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

package io.renku.eventlog.subscriptions.commitsync

import cats.effect.{ContextShift, IO, Timer}
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.subscriptions._
import io.renku.eventlog.subscriptions.commitsync.CommitSyncEventEncoder.encodeEvent
import io.renku.eventlog.{EventLogDB, subscriptions}
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[subscriptions] object SubscriptionCategory {

  def apply(sessionResource:   SessionResource[IO, EventLogDB],
            queriesExecTimes:  LabeledHistogram[IO, SqlStatement.Name],
            subscriberTracker: SubscriberTracker[IO],
            logger:            Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[subscriptions.SubscriptionCategory[IO]] = for {
    subscribers      <- Subscribers(categoryName, subscriberTracker, logger)
    eventsFinder     <- CommitSyncEventFinder(sessionResource, queriesExecTimes)
    dispatchRecovery <- LoggingDispatchRecovery[IO, CommitSyncEvent](categoryName, logger)
    eventDelivery    <- EventDelivery.noOp[IO, CommitSyncEvent]
    eventsDistributor <- IOEventsDistributor(categoryName,
                                             subscribers,
                                             eventsFinder,
                                             eventDelivery,
                                             EventEncoder(encodeEvent),
                                             dispatchRecovery,
                                             logger
                         )
    deserializer <-
      SubscriptionRequestDeserializer[IO, SubscriptionCategoryPayload](categoryName, SubscriptionCategoryPayload.apply)
  } yield new SubscriptionCategoryImpl[IO, SubscriptionCategoryPayload](categoryName,
                                                                        subscribers,
                                                                        eventsDistributor,
                                                                        deserializer
  )
}
