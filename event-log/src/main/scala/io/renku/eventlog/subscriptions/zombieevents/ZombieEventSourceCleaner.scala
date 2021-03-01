package io.renku.eventlog.subscriptions.zombieevents

import cats.effect.{Bracket, ContextShift, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.MicroserviceBaseUrl
import eu.timepit.refined.api.Refined
import io.renku.eventlog.{EventLogDB, TypeSerializers}

private trait ZombieEventSourceCleaner[Interpretation[_]] {
  def removeZombieSources(): Interpretation[Unit]
}

private class ZombieEventSourceCleanerImpl(transactor:          DbTransactor[IO, EventLogDB],
                                           queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name],
                                           microserviceBaseUrl: MicroserviceBaseUrl,
                                           serviceHealth:       ServiceHealth[IO]
)(implicit ME:                                                  Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with ZombieEventSourceCleaner[IO]
    with TypeSerializers {

  import doobie.implicits._

  override def removeZombieSources(): IO[Unit] = findPotentialZombieSources.void

  private def findPotentialZombieSources: IO[List[SubscriberUrl]] = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT DISTINCT source_url
            |FROM subscriber
            |WHERE source_url <> $microserviceBaseUrl
    """.stripMargin
        .query[SubscriberUrl]
        .to[List],
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find zombie sources")
    )
  } transact transactor.get
}

object ZombieEventSourceCleaner {}
