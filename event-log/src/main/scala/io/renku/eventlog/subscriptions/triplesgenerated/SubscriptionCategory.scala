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

package io.renku.eventlog.subscriptions.triplesgenerated

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger
import io.renku.eventlog.subscriptions._
import io.renku.eventlog.{EventLogDB, subscriptions}

import scala.concurrent.ExecutionContext

private[subscriptions] object SubscriptionCategory {
  val name: CategoryName = CategoryName("TRIPLES_GENERATED")

  def apply(
      transactor:                  SessionResource[IO, EventLogDB],
      awaitingTransformationGauge: LabeledGauge[IO, projects.Path],
      underTransformationGauge:    LabeledGauge[IO, projects.Path],
      queriesExecTimes:            LabeledHistogram[IO, SqlQuery.Name],
      subscriberTracker:           SubscriberTracker[IO],
      logger:                      Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[subscriptions.SubscriptionCategory[IO]] = for {
    subscribers <- Subscribers(name, subscriberTracker, logger)
    eventFetcher <-
      IOTriplesGeneratedEventFinder(transactor, awaitingTransformationGauge, underTransformationGauge, queriesExecTimes)
    dispatchRecovery <-
      DispatchRecovery(transactor, awaitingTransformationGauge, underTransformationGauge, queriesExecTimes, logger)
    eventDelivery <- EventDelivery[TriplesGeneratedEvent](transactor,
                                                          compoundEventIdExtractor = (_: TriplesGeneratedEvent).id,
                                                          queriesExecTimes
                     )
    eventsDistributor <- IOEventsDistributor(name,
                                             transactor,
                                             subscribers,
                                             eventFetcher,
                                             eventDelivery,
                                             TriplesGeneratedEventEncoder,
                                             dispatchRecovery,
                                             logger
                         )
    deserializer <-
      SubscriptionRequestDeserializer[IO, SubscriptionCategoryPayload](name, SubscriptionCategoryPayload.apply)
  } yield new SubscriptionCategoryImpl[IO, SubscriptionCategoryPayload](name,
                                                                        subscribers,
                                                                        eventsDistributor,
                                                                        deserializer
  )
}
