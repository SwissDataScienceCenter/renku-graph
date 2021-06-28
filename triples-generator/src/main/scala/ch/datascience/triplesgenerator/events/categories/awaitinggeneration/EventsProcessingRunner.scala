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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration

import cats.effect.IO._
import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.syntax.all._
import ch.datascience.events.consumers.EventSchedulingResult
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.metrics.MetricsRegistry
import com.typesafe.config.{Config, ConfigFactory}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait EventsProcessingRunner[Interpretation[_]] {
  def scheduleForProcessing(
      event:                CommitEvent,
      currentSchemaVersion: SchemaVersion
  ): Interpretation[EventSchedulingResult]
}

private class EventsProcessingRunnerImpl[Interpretation[_]: Concurrent](
    eventProcessor:        EventProcessor[Interpretation],
    generationProcesses:   GenerationProcessesNumber,
    semaphore:             Semaphore[Interpretation],
    subscriptionMechanism: SubscriptionMechanism[Interpretation],
    logger:                Logger[Interpretation]
) extends EventsProcessingRunner[Interpretation] {

  import subscriptionMechanism._

  override def scheduleForProcessing(event:                CommitEvent,
                                     currentSchemaVersion: SchemaVersion
  ): Interpretation[EventSchedulingResult] = semaphore.available flatMap {
    case 0 => Busy.pure[Interpretation].widen[EventSchedulingResult]
    case _ =>
      {
        for {
          _ <- semaphore.acquire
          _ <- Concurrent[Interpretation].start(process(event, currentSchemaVersion))
        } yield Accepted: EventSchedulingResult
      } recoverWith releasingSemaphore
  }

  private def process(event: CommitEvent, currentSchemaVersion: SchemaVersion) = {
    for {
      _ <- eventProcessor.process(event, currentSchemaVersion)
      _ <- releaseAndNotify()
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    for {
      _ <- releaseAndNotify()
      _ <- logger.error(exception)(s"Processing event ${event.compoundEventId} failed")
    } yield ()
  }

  private def releasingSemaphore[O]: PartialFunction[Throwable, Interpretation[O]] = { case NonFatal(exception) =>
    semaphore.available flatMap {
      case available if available == generationProcesses.value => exception.raiseError[Interpretation, O]
      case _ =>
        semaphore.release flatMap { _ =>
          exception.raiseError[Interpretation, O]
        }
    }
  }

  private def releaseAndNotify(): Interpretation[Unit] = for {
    _ <- semaphore.release
    _ <- Concurrent[Interpretation].start(renewSubscription())
  } yield ()
}

private object IOEventsProcessingRunner {

  def apply(
      metricsRegistry:       MetricsRegistry[IO],
      subscriptionMechanism: SubscriptionMechanism[IO],
      logger:                Logger[IO],
      config:                Config = ConfigFactory.load()
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventsProcessingRunner[IO]] =
    for {
      eventProcessor      <- IOCommitEventProcessor(metricsRegistry, logger)
      generationProcesses <- GenerationProcessesNumber(config)
      semaphore           <- Semaphore(generationProcesses.value)
    } yield new EventsProcessingRunnerImpl(eventProcessor,
                                           generationProcesses,
                                           semaphore,
                                           subscriptionMechanism,
                                           logger
    )
}
