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

package ch.datascience.triplesgenerator
package events.categories.awaitinggeneration

import cats.MonadThrow
import cats.data.EitherT.{fromEither, fromOption}
import cats.data.NonEmptyList
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.consumers.{ConcurrentProcessesLimiter, EventRequestContent, EventSchedulingResult}
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventBody}
import ch.datascience.metrics.MetricsRegistry
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[events] class EventHandler[Interpretation[_]: MonadThrow: ConcurrentEffect: ContextShift](
    override val categoryName:  CategoryName,
    eventProcessor:             EventProcessor[Interpretation],
    eventBodyDeserializer:      EventBodyDeserializer[Interpretation],
    subscriptionMechanism:      SubscriptionMechanism[Interpretation],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[Interpretation],
    currentVersionPair:         RenkuVersionPair,
    logger:                     Logger[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](concurrentProcessesLimiter) {

  import currentVersionPair.schemaVersion
  import eventBodyDeserializer.toCommitEvents

  override def maybeReleaseProcess: Option[Interpretation[Unit]] =
    Concurrent[Interpretation].start(subscriptionMechanism.renewSubscription()).void.some

  override def handle(
      requestContent: EventRequestContent
  ): Interpretation[(Deferred[Interpretation, Unit], Interpretation[EventSchedulingResult])] =
    Deferred[Interpretation, Unit].map(done => done -> startProcessEvent(requestContent, done))

  private def startProcessEvent(requestContent: EventRequestContent, done: Deferred[Interpretation, Unit]) = {
    for {
      eventId      <- fromEither(requestContent.event.getEventId)
      eventBody    <- fromOption[Interpretation](requestContent.maybePayload.map(EventBody.apply), BadRequest)
      commitEvents <- toCommitEvents(eventBody).toRightT(recoverTo = BadRequest)
      result <-
        (ContextShift[Interpretation].shift *> Concurrent[Interpretation]
          .start(eventProcessor.process(eventId, commitEvents, schemaVersion).flatMap(_ => done.complete(())))).toRightT
          .map(_ => Accepted)
          .semiflatTap(logger.log(eventId -> commitEvents))
          .leftSemiflatTap(logger.log(eventId -> commitEvents))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: ((CompoundEventId, NonEmptyList[CommitEvent])) => String = {
    case (eventId, events) => s"$eventId, projectPath = ${events.head.project.path}"
  }
}

object EventHandler {

  def apply(
      currentVersionPair:    RenkuVersionPair,
      metricsRegistry:       MetricsRegistry[IO],
      subscriptionMechanism: SubscriptionMechanism[IO],
      logger:                Logger[IO],
      config:                Config = ConfigFactory.load()
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    eventProcessor           <- IOCommitEventProcessor(metricsRegistry, logger)
    generationProcesses      <- GenerationProcessesNumber[IO](config)
    concurrentProcessLimiter <- ConcurrentProcessesLimiter(Refined.unsafeApply(generationProcesses.value))
  } yield new EventHandler[IO](categoryName,
                               eventProcessor,
                               EventBodyDeserializer(),
                               subscriptionMechanism,
                               concurrentProcessLimiter,
                               currentVersionPair,
                               logger
  )
}
