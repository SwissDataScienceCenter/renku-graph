package ch.datascience.commiteventservice.events.categories.globalcommitsync

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.commiteventservice.Microservice
import ch.datascience.commiteventservice.events.categories.globalcommitsync.EventHandler
import ch.datascience.commiteventservice.events.categories.globalcommitsync.{EventHandler, categoryName}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.consumers.subscriptions.SubscriptionPayloadComposer.categoryAndUrlPayloadsComposerFactory
import ch.datascience.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

object SubscriptionFactory {

  def apply(gitLabThrottler:       Throttler[IO, GitLab],
            logger:                Logger[IO],
            executionTimeRecorder: ExecutionTimeRecorder[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[(EventHandler[IO], SubscriptionMechanism[IO])] = for {
    subscriptionMechanism <- SubscriptionMechanism(
                               categoryName,
                               categoryAndUrlPayloadsComposerFactory(Microservice.ServicePort, Microservice.Identifier),
                               logger
                             )
    handler <- EventHandler(gitLabThrottler, executionTimeRecorder, logger)
  } yield handler -> subscriptionMechanism
}
