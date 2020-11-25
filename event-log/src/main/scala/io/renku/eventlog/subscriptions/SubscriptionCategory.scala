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

import cats.data.OptionT
import cats.effect.{ContextShift, Effect, IO, Timer}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.circe.Json
import io.renku.eventlog.EventLogDB

import scala.concurrent.ExecutionContext

trait SubscriptionCategory[Interpretation[_], T] {
  def run(): Interpretation[Unit]

  def register(payload: Json): Interpretation[Option[T]]
}

class SubscriptionCategoryUnprocessed[Interpretation[_]: Effect](
    subscribers:       Subscribers[Interpretation],
    eventsDistributor: EventsDistributor[Interpretation],
    deserializer:      SubscriptionRequestDeserializer[Interpretation, SubscriberUrl]
) extends SubscriptionCategory[Interpretation, SubscriberUrl] {
  override def run(): Interpretation[Unit] = eventsDistributor.run()

  override def register(payload: Json): Interpretation[Option[SubscriberUrl]] = (for {
    subscriberUrl <- OptionT(deserializer.deserialize(payload))
    _             <- OptionT.liftF(subscribers.add(subscriberUrl))
  } yield subscriberUrl).value
}

object IOSubscriptionCategoryUnprocessed {
  def apply(transactor:           DbTransactor[IO, EventLogDB],
            waitingEventsGauge:   LabeledGauge[IO, projects.Path],
            underProcessingGauge: LabeledGauge[IO, projects.Path],
            queriesExecTimes:     LabeledHistogram[IO, SqlQuery.Name],
            logger:               Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[SubscriptionCategory[IO, SubscriberUrl]] = for {
    subscribers <- Subscribers(logger)
    eventsDistributor <-
      IOEventsDistributor(transactor, subscribers, waitingEventsGauge, underProcessingGauge, queriesExecTimes, logger)
  } yield {
    val deserializer = new unprocessed.SubscriptionRequestDeserializer[IO]()
    new SubscriptionCategoryUnprocessed[IO](subscribers, eventsDistributor, deserializer)
  }

}
