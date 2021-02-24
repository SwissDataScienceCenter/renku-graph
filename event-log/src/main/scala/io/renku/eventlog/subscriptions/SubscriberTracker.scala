package io.renku.eventlog.subscriptions

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.IOMicroserviceUrlFinder
import doobie.implicits._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait SubscriberTracker[Interpretation[_]] {
  def add(subscriberUrl:    SubscriberUrl): Interpretation[Unit]
  def remove(subscriberUrl: SubscriberUrl): Interpretation[Unit]
}

private class SubscriberTrackerImpl(transactor:       DbTransactor[IO, EventLogDB],
                                    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
                                    sourceUrl:        SubscriberUrl,
                                    logger:           Logger[IO]
)(implicit ME:                                        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with SubscriberTracker[IO] {
  override def add(subscriberUrl: SubscriberUrl): IO[Unit] = measureExecutionTime(
    SqlQuery(
      sql"""INSERT INTO
           |subscriber (delivery_url, source_url)
           |VALUES (${subscriberUrl.value}, ${sourceUrl.value})
           |WHERE NOT EXISTS(select * from subscriber WHERE delivery_url=${subscriberUrl.value}, source_url=${sourceUrl.value})""".stripMargin.update.run,
      name = "subscriber - add"
    )
  ) transact transactor.get

  override def remove(subscriberUrl: SubscriberUrl): IO[Unit] = ???
}

private object SubscriberTracker {
  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            microservicePort: Int Refined Positive,
            logger:           Logger[IO]
  ): IO[SubscriberTracker[IO]] = for {
    subscriptionUrlFinder <- IOMicroserviceUrlFinder(microservicePort)
    sourceUrl             <- subscriptionUrlFinder.findSubscriberUrl()
  } yield new SubscriberTrackerImpl(transactor, queriesExecTimes, sourceUrl, logger)
}
