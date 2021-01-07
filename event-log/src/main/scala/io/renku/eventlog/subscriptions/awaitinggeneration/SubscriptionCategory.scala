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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.subscriptions.SubscriptionCategory.CategoryName
import io.renku.eventlog.subscriptions.{IOEventsDistributor, Subscribers, SubscriptionCategoryImpl}
import io.renku.eventlog.{EventLogDB, subscriptions}

import scala.concurrent.ExecutionContext

private[subscriptions] object SubscriptionCategory {

  val name: CategoryName = CategoryName("AWAITING_GENERATION")

  def apply(
      transactor:                  DbTransactor[IO, EventLogDB],
      waitingEventsGauge:          LabeledGauge[IO, projects.Path],
      underTriplesGenerationGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:            LabeledHistogram[IO, SqlQuery.Name],
      logger:                      Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[subscriptions.SubscriptionCategory[IO]] = for {
    subscribers <- Subscribers(logger)
    eventFetcher <-
      IOAwaitingGenerationEventFetcher(transactor, waitingEventsGauge, underTriplesGenerationGauge, queriesExecTimes)
    dispatchRecovery <- DispatchRecovery(transactor, underTriplesGenerationGauge, queriesExecTimes, logger)
    eventsDistributor <-
      IOEventsDistributor(transactor,
                          subscribers,
                          eventFetcher,
                          AwaitingGenerationEventEncoder,
                          dispatchRecovery,
                          logger
      )
    deserializer = SubscriptionRequestDeserializer[IO]()
  } yield new SubscriptionCategoryImpl[IO, SubscriptionCategoryPayload](name,
                                                                        subscribers,
                                                                        eventsDistributor,
                                                                        deserializer
  )
}
