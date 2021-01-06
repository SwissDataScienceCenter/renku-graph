package io.renku.eventlog.subscriptions.membersync

import cats.effect.{ContextShift, IO}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.EventFinder

private class MemberSyncEventsFinderImpl(transactor:       DbTransactor[IO, EventLogDB],
                                         queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
) {}

private object MemberSyncEventsFinder {
  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, MemberSyncEvent]] = ???
}
