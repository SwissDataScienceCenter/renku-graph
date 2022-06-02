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

package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.kernel.Deferred
import cats.effect.{Async, Concurrent, Spawn}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import eu.timepit.refined.api.Refined
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQueryTimeRecorder
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
  ): F[EventHandlingProcess[F]] = EventHandlingProcess.withWaitingForCompletion[F](
    deferred => startCleanUp(requestContent, deferred),
    subscriptionMechanism.renewSubscription()
  )

  private def startCleanUp(requestContent: EventRequestContent, deferred: Deferred[F, Unit]) = for {
    cleanupEvent <- toCleanUpEvent(requestContent.event).toRightT(recoverTo = BadRequest)
    result <- Spawn[F]
                .start(eventProcessor.process(cleanupEvent.project) >> deferred.complete(()))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(cleanupEvent))
                .leftSemiflatTap(Logger[F].log(cleanupEvent))
  } yield result

  private implicit lazy val eventInfoToString: Show[CleanUpEvent] = Show.show { event =>
    show"projectId = ${event.project.id}, projectPath = ${event.project.path}"
  }
}

object EventHandler {

  private val singleProcess = 1

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      subscriptionMechanism: SubscriptionMechanism[F]
  ): F[EventHandler[F]] = for {
    concurrentProcessLimiter <- ConcurrentProcessesLimiter(Refined.unsafeApply(singleProcess))
    eventProcessor           <- EventProcessor[F]
  } yield new EventHandler[F](categoryName,
                              eventProcessor,
                              EventBodyDeserializer[F],
                              subscriptionMechanism,
                              concurrentProcessLimiter
  )
}
