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
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.circe.Json
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.unprocessed.IOUnprocessedEventFetcher

import scala.concurrent.ExecutionContext

trait SubscriptionCategoryRegistry[Interpretation[_]] {

  def run(): Interpretation[Unit]

  def register(subscriptionRequest: Json): Interpretation[Either[RequestError, Unit]]
}

private[subscriptions] class SubscriptionCategoryRegistryImpl[Interpretation[_]](
    categories: Set[SubscriptionCategory[Interpretation]]
) extends SubscriptionCategoryRegistry[Interpretation] {
  override def run(): Interpretation[Unit] = ??? // inst

  override def register(subscriptionRequest: Json): Interpretation[Either[RequestError, Unit]] = ???
}

private[eventlog] object IOSubscriptionCategoryRegistry {
  def apply(
      transactor:           DbTransactor[IO, EventLogDB],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:     LabeledHistogram[IO, SqlQuery.Name],
      logger:               Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[SubscriptionCategoryRegistry[IO]] =
    for {

      subscribers <- Subscribers(ApplicationLogger)
      eventFetcher <-
        IOUnprocessedEventFetcher(transactor, waitingEventsGauge, underProcessingGauge, queriesExecTimes)
      eventDistributor <-
        IOEventsDistributor(transactor,
                            subscribers,
                            eventFetcher,
                            underProcessingGauge,
                            queriesExecTimes,
                            ApplicationLogger
        )
      deserializer = unprocessed.SubscriptionRequestDeserializer[IO]()
      unprocessedCategory <-
        IOSubscriptionCategory(subscribers, eventDistributor, deserializer)
      otherCategory <-
        IOSubscriptionCategory(subscribers, eventDistributor, deserializer)
    } yield new SubscriptionCategoryRegistryImpl(Set(unprocessedCategory))

}
