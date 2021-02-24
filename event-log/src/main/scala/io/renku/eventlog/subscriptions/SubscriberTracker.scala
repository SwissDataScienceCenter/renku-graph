package io.renku.eventlog.subscriptions

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import ch.datascience.tinytypes.constraints.Url
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

final class ServerUrl private (val value: String) extends AnyVal with StringTinyType
object ServerUrl extends TinyTypeFactory[ServerUrl](new ServerUrl(_)) with Url

private trait SubscriberTracker[Interpretation[_]] {
  def add(subscriberUrl:    SubscriptionInfo, serverUrl: ServerUrl): Interpretation[Unit]
  def remove(subscriberUrl: SubscriptionInfo, serverUrl: ServerUrl): Interpretation[Unit]
}

private class SubscriberTrackerImpl(transactor:       DbTransactor[IO, EventLogDB],
                                    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
                                    logger:           Logger[IO]
)(implicit ME:                                        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with SubscriberTracker[IO] {
  override def add(subscriberUrl: SubscriptionInfo, serverUrl: ServerUrl): IO[Unit] = ???

  override def remove(subscriberUrl: SubscriptionInfo, serverUrl: ServerUrl): IO[Unit] = ???
}

private object SubscriberTracker {
  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            logger:           Logger[IO]
  ): IO[SubscriberTracker[IO]] = IO(new SubscriberTrackerImpl(transactor, queriesExecTimes, logger))
}
