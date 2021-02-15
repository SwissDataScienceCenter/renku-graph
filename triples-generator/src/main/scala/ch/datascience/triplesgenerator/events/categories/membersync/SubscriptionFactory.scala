package ch.datascience.triplesgenerator.events.categories.membersync

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionMechanism
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionPayloadComposer.categoryAndUrlPayloadsComposerFactory
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

object SubscriptionFactory {

  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(
      implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[(EventHandler[IO], SubscriptionMechanism[IO])] =
    for {
      subscriptionMechanism <- SubscriptionMechanism(categoryName, categoryAndUrlPayloadsComposerFactory, logger)
      handler               <- EventHandler(gitLabThrottler, logger, timeRecorder)
    } yield handler -> subscriptionMechanism
}
