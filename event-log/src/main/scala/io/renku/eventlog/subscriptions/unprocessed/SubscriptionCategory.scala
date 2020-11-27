package io.renku.eventlog.subscriptions.unprocessed

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.{IOEventsDistributor, Subscribers, SubscriptionCategory, SubscriptionCategoryImpl}

import scala.concurrent.ExecutionContext

private[subscriptions] object SubscriptionCategory {
  def apply(
      transactor:           DbTransactor[IO, EventLogDB],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:     LabeledHistogram[IO, SqlQuery.Name],
      logger:               Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[SubscriptionCategory[IO]] = for {
    subscribers <- Subscribers(logger)
    eventFetcher <-
      IOUnprocessedEventFetcher(transactor, waitingEventsGauge, underProcessingGauge, queriesExecTimes)
    eventsDistributor <-
      IOEventsDistributor(transactor, subscribers, eventFetcher, underProcessingGauge, queriesExecTimes, logger)
    deserializer = SubscriptionRequestDeserializer[IO]()
  } yield new SubscriptionCategoryImpl[IO, SubscriptionCategoryPayload](subscribers, eventsDistributor, deserializer)
}
