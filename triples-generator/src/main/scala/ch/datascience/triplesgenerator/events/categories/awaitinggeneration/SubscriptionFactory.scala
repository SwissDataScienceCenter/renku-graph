package ch.datascience.triplesgenerator
package events.categories.awaitinggeneration

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.subscriptions.PayloadComposer.payloadsComposerFactory
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionMechanism
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

object SubscriptionFactory {
  def apply(currentVersionPair: RenkuVersionPair,
            metricsRegistry:    MetricsRegistry[IO],
            gitLabThrottler:    Throttler[IO, GitLab],
            timeRecorder:       SparqlQueryTimeRecorder[IO],
            logger:             Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[(EventHandler[IO], SubscriptionMechanism[IO])] =
    for {
      subscriptionMechanism <- SubscriptionMechanism(categoryName, payloadsComposerFactory, logger)
      handler <-
        EventHandler(currentVersionPair, metricsRegistry, gitLabThrottler, timeRecorder, subscriptionMechanism, logger)
    } yield handler -> subscriptionMechanism
}
