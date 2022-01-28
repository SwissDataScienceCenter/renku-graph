/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.triplesgenerator.events.categories.awaitinggeneration

import cats.data.EitherT
import cats.effect.kernel.Deferred
import cats.effect.{Async, Concurrent}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.{CategoryName, EventBody}
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private[events] class EventHandler[F[_]: MonadThrow: Concurrent: Logger](
    override val categoryName:  CategoryName,
    eventProcessor:             EventProcessor[F],
    eventBodyDeserializer:      EventBodyDeserializer[F],
    subscriptionMechanism:      SubscriptionMechanism[F],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[F]
) extends consumers.EventHandlerWithProcessLimiter[F](concurrentProcessesLimiter) {

  import eventBodyDeserializer.toCommitEvent

  override def createHandlingProcess(
      requestContent: EventRequestContent
  ): F[EventHandlingProcess[F]] =
    EventHandlingProcess.withWaitingForCompletion[F](
      deferred => startProcessEvent(requestContent, deferred),
      subscriptionMechanism.renewSubscription()
    )

  private def startProcessEvent(requestContent: EventRequestContent, deferred: Deferred[F, Unit]) = for {
    eventBody <- requestContent match {
                   case EventRequestContent.WithPayload(_, payload: String) => EitherT.rightT(EventBody(payload))
                   case _                                                   => EitherT.leftT(BadRequest)
                 }
    commitEvent <- toCommitEvent(eventBody).toRightT(recoverTo = BadRequest)
    result <- Concurrent[F]
                .start(eventProcessor.process(commitEvent) >> deferred.complete(()))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(commitEvent))
                .leftSemiflatTap(Logger[F].log(commitEvent))
  } yield result

  private implicit lazy val eventInfoToString: Show[CommitEvent] = Show.show { event =>
    show"${event.compoundEventId}, projectPath = ${event.project.path}"
  }
}

object EventHandler {

  def apply[F[_]: Async: Logger](
      metricsRegistry:       MetricsRegistry,
      subscriptionMechanism: SubscriptionMechanism[F],
      config:                Config = ConfigFactory.load()
  ): F[EventHandler[F]] = for {
    eventProcessor           <- CommitEventProcessor(metricsRegistry)
    generationProcesses      <- GenerationProcessesNumber[F](config)
    concurrentProcessLimiter <- ConcurrentProcessesLimiter(Refined.unsafeApply(generationProcesses.value))
  } yield new EventHandler[F](categoryName,
                              eventProcessor,
                              EventBodyDeserializer[F],
                              subscriptionMechanism,
                              concurrentProcessLimiter
  )
}
