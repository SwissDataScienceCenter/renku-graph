package io.renku.eventlog.subscriptions.triplesgenerated

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.SubscriptionCategory.CategoryName
import io.renku.eventlog.subscriptions.{IOEventsDistributor, Subscribers, SubscriptionCategoryImpl, SubscriptionCategoryPayload, SubscriptionRequestDeserializer}
import io.renku.eventlog.{EventLogDB, subscriptions}
import scala.concurrent.ExecutionContext

private[subscriptions] object SubscriptionCategory {
  val name: CategoryName = CategoryName("TRIPLES_GENERATED")

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
    subscribers <- Subscribers(name, logger)
    eventFetcher <-
      IOTriplesGeneratedEventFinder(transactor, waitingEventsGauge, underTriplesGenerationGauge, queriesExecTimes)
    dispatchRecovery <- DispatchRecovery(transactor, underTriplesGenerationGauge, queriesExecTimes, logger)
    eventsDistributor <- IOEventsDistributor(name,
                                             transactor,
                                             subscribers,
                                             eventFetcher,
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
