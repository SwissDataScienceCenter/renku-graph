package io.renku.eventlog.subscriptions.membersync

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.subscriptions.{IOEventsDistributor, Subscribers, SubscriptionCategoryImpl}
import io.renku.eventlog.{EventLogDB, subscriptions}

import scala.concurrent.ExecutionContext

private[subscriptions] object SubscriptionCategory {
  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[subscriptions.SubscriptionCategory[IO]] = ???
//  for {
//    subscribers       <- Subscribers(logger)
//    eventFinder       <- MemberSyncEventsFinder(transactor, queriesExecTimes)
//    eventsDistributor <- IOEventsDistributor(transactor, subscribers, eventFinder, queriesExecTimes, logger)
//    deserializer = SubscriptionRequestDeserializer[IO]()
//  } yield new SubscriptionCategoryImpl[IO, SubscriptionCategoryPayload](subscribers, eventsDistributor, deserializer)
}
