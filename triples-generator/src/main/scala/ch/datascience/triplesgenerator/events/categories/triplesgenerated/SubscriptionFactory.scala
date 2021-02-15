package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionMechanism
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionPayloadComposer.categoryAndUrlPayloadsComposerFactory
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

object SubscriptionFactory {
  def apply(
      metricsRegistry: MetricsRegistry[IO],
      gitLabThrottler: Throttler[IO, GitLab],
      timeRecorder:    SparqlQueryTimeRecorder[IO],
      logger:          Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[(EventHandler[IO], SubscriptionMechanism[IO])] =
    for {
      subscriptionMechanism <- SubscriptionMechanism(categoryName, categoryAndUrlPayloadsComposerFactory, logger)
      handler               <- EventHandler(metricsRegistry, gitLabThrottler, timeRecorder, subscriptionMechanism, logger)
    } yield handler -> subscriptionMechanism
}
