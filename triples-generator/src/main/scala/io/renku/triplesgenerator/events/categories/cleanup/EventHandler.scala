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

package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.{Async, Concurrent, Spawn}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.CategoryName
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private[events] class EventHandler[F[_]: MonadThrow: Concurrent: Logger](
    override val categoryName:  CategoryName,
    eventProcessor:             EventProcessor[F],
    eventBodyDeserializer:      EventBodyDeserializer[F],
    subscriptionMechanism:      SubscriptionMechanism[F],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[F]
) extends consumers.EventHandlerWithProcessLimiter[F](concurrentProcessesLimiter) {

  import eventBodyDeserializer.toCleanUpEvent

  override def createHandlingProcess(
      requestContent: EventRequestContent
  ): F[EventHandlingProcess[F]] = EventHandlingProcess[F](startCleanUp(requestContent))

  private def startCleanUp(requestContent: EventRequestContent) = for {
    cleanupEvent <- toCleanUpEvent(requestContent.event).toRightT(recoverTo = BadRequest)
    result <- Spawn[F]
                .start(eventProcessor.process(cleanupEvent))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(cleanupEvent.project.path))
                .leftSemiflatTap(Logger[F].log(cleanupEvent.project.path))
  } yield result

  private implicit lazy val eventInfoToString: Show[CleanUpEvent] = Show.show { event =>
    show"projectId = ${event.project.id}, projectPath = ${event.project.path}"
  }
}

object EventHandler {

  private val singleProcess = 1

  def apply[F[_]: Async: Logger](
      metricsRegistry:       MetricsRegistry,
      subscriptionMechanism: SubscriptionMechanism[F],
      config:                Config = ConfigFactory.load()
  ): F[EventHandler[F]] = for {
    concurrentProcessLimiter <- ConcurrentProcessesLimiter(Refined.unsafeApply(singleProcess))
    eventProcessor           <- CleanUpEventProcessor[F]()
  } yield new EventHandler[F](categoryName,
                              eventProcessor,
                              EventBodyDeserializer[F],
                              subscriptionMechanism,
                              concurrentProcessLimiter
  )
}
