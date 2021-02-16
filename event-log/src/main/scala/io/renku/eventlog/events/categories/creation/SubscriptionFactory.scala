package io.renku.eventlog.events.categories.creation

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.events.consumers.EventHandler
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.consumers.subscriptions.SubscriptionPayloadComposer.categoryAndUrlPayloadsComposerFactory
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.{EventLogDB, Microservice}

import scala.concurrent.ExecutionContext

object SubscriptionFactory {

  def apply(transactor:         DbTransactor[IO, EventLogDB],
            waitingEventsGauge: LabeledGauge[IO, projects.Path],
            queriesExecTimes:   LabeledHistogram[IO, SqlQuery.Name],
            logger:             Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[(EventHandler[IO], SubscriptionMechanism[IO])] = for {
    subscriptionMechanism <-
      SubscriptionMechanism(categoryName, categoryAndUrlPayloadsComposerFactory(Microservice.ServicePort), logger)
    handler <- EventHandler(transactor, waitingEventsGauge, queriesExecTimes, logger)
  } yield handler -> subscriptionMechanism
}
