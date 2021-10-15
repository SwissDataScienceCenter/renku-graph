/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import ch.datascience.events.{EventRequestContent, consumers}
import ch.datascience.graph.model.events.{CategoryName, EventBody}
import ch.datascience.metrics.MetricsRegistry
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[events] class EventHandler[Interpretation[_]: MonadThrow: ConcurrentEffect: ContextShift: Logger](
    override val categoryName:  CategoryName,
    eventProcessor:             EventProcessor[Interpretation],
    eventBodyDeserializer:      EventBodyDeserializer[Interpretation],
    subscriptionMechanism:      SubscriptionMechanism[Interpretation],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](concurrentProcessesLimiter) {

  import eventBodyDeserializer.toCommitEvent

  override def createHandlingProcess(
      requestContent: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    EventHandlingProcess.withWaitingForCompletion[Interpretation](
      deferred => startProcessEvent(requestContent, deferred),
      subscriptionMechanism.renewSubscription()
    )

  private def startProcessEvent(requestContent: EventRequestContent, deferred: Deferred[Interpretation, Unit]) = for {
    eventBody <- requestContent match {
                   case EventRequestContent.WithPayload(_, payload: String) => EitherT.rightT(EventBody(payload))
                   case _ => EitherT.leftT(BadRequest)
                 }
    commitEvent <- toCommitEvent(eventBody).toRightT(recoverTo = BadRequest)
    result <- Concurrent[Interpretation]
                .start(eventProcessor.process(commitEvent) >> deferred.complete(()))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[Interpretation].log(commitEvent))
                .leftSemiflatTap(Logger[Interpretation].log(commitEvent))
  } yield result

  private implicit lazy val eventInfoToString: Show[CommitEvent] = Show.show { event =>
    show"${event.compoundEventId}, projectPath = ${event.project.path}"
  }
}

object EventHandler {

  def apply(
      metricsRegistry:       MetricsRegistry[IO],
      subscriptionMechanism: SubscriptionMechanism[IO],
      config:                Config = ConfigFactory.load()
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO],
      logger:           Logger[IO]
  ): IO[EventHandler[IO]] = for {
    eventProcessor           <- IOCommitEventProcessor(metricsRegistry)
    generationProcesses      <- GenerationProcessesNumber[IO](config)
    concurrentProcessLimiter <- ConcurrentProcessesLimiter(Refined.unsafeApply(generationProcesses.value))
  } yield new EventHandler[IO](categoryName,
                               eventProcessor,
                               EventBodyDeserializer(),
                               subscriptionMechanism,
                               concurrentProcessLimiter
  )
}
