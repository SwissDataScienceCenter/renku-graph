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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.effect.IO._
import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.syntax.all._
import ch.datascience.config.{ConfigLoader, GitLab}
import ch.datascience.control.Throttler
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.events.EventSchedulingResult
import ch.datascience.triplesgenerator.events.EventSchedulingResult._
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionMechanism
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait EventsProcessingRunner[Interpretation[_]] {
  def scheduleForProcessing(
      triplesGeneratedEvent: TriplesGeneratedEvent,
      currentSchemaVersion:  SchemaVersion
  ): Interpretation[EventSchedulingResult]
}

private class EventsProcessingRunnerImpl(
    eventProcessor:        EventProcessor[IO],
    generationProcesses:   Long Refined Positive,
    semaphore:             Semaphore[IO],
    subscriptionMechanism: SubscriptionMechanism[IO],
    logger:                Logger[IO]
)(implicit cs:             ContextShift[IO])
    extends EventsProcessingRunner[IO] {

  import subscriptionMechanism._

  override def scheduleForProcessing(
      triplesGeneratedEvent: TriplesGeneratedEvent,
      currentSchemaVersion:  SchemaVersion
  ): IO[EventSchedulingResult] =
    semaphore.available flatMap {
      case 0 => Busy.pure[IO]
      case _ =>
        {
          for {
            _ <- semaphore.acquire
            _ <- process(triplesGeneratedEvent, currentSchemaVersion).start
          } yield Accepted: EventSchedulingResult
        } recoverWith releasingSemaphore
    }

  private def process(triplesGeneratedEvent: TriplesGeneratedEvent, currentSchemaVersion: SchemaVersion) = {
    for {
      _ <- eventProcessor.process(triplesGeneratedEvent, currentSchemaVersion)
      _ <- releaseAndNotify()
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    for {
      _ <- releaseAndNotify()
      _ <- logger.error(exception)(s"Processing event ${triplesGeneratedEvent.compoundEventId} failed")
    } yield ()
  }

  private def releasingSemaphore[O]: PartialFunction[Throwable, IO[O]] = { case NonFatal(exception) =>
    semaphore.available flatMap {
      case available if available == generationProcesses.value => exception.raiseError[IO, O]
      case _ =>
        semaphore.release flatMap { _ =>
          exception.raiseError[IO, O]
        }
    }
  }

  private def releaseAndNotify(): IO[Unit] =
    for {
      _ <- semaphore.release
      _ <- renewSubscription().start
    } yield ()
}

private object IOEventsProcessingRunner {

  import ConfigLoader.find
  import eu.timepit.refined.pureconfig._

  import scala.language.postfixOps

  def apply(
      metricsRegistry:       MetricsRegistry[IO],
      gitLabThrottler:       Throttler[IO, GitLab],
      timeRecorder:          SparqlQueryTimeRecorder[IO],
      subscriptionMechanism: SubscriptionMechanism[IO],
      logger:                Logger[IO],
      config:                Config = ConfigFactory.load()
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventsProcessingRunner[IO]] =
    for {
      eventProcessor      <- IOTriplesGeneratedEventProcessor(metricsRegistry, gitLabThrottler, timeRecorder, logger)
      generationProcesses <- find[IO, Long Refined Positive]("transformation-processes-number", config)
      semaphore           <- Semaphore(generationProcesses.value)
    } yield new EventsProcessingRunnerImpl(eventProcessor,
                                           generationProcesses,
                                           semaphore,
                                           subscriptionMechanism,
                                           logger
    )
}
