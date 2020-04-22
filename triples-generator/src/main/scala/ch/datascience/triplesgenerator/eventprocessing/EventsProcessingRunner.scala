/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import cats.data.NonEmptyList
import cats.effect.IO._
import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import ch.datascience.config.ConfigLoader
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.triplesgenerator.eventprocessing.EventsProcessingRunner.EventSchedulingResult
import ch.datascience.triplesgenerator.eventprocessing.EventsProcessingRunner.EventSchedulingResult._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds
import scala.util.control.NonFatal

private trait EventsProcessingRunner[Interpretation[_]] {
  def scheduleForProcessing(eventId: CompoundEventId,
                            events:  NonEmptyList[CommitEvent]): Interpretation[EventSchedulingResult]
}

object EventsProcessingRunner {

  sealed trait EventSchedulingResult extends Product with Serializable

  object EventSchedulingResult {
    case object Accepted extends EventSchedulingResult
    case object Busy     extends EventSchedulingResult
  }
}

private class IOEventsProcessingRunner private (
    eventProcessor: EventProcessor[IO],
    semaphore:      Semaphore[IO],
    logger:         Logger[IO]
)(implicit cs:      ContextShift[IO])
    extends EventsProcessingRunner[IO] {

  override def scheduleForProcessing(eventId: CompoundEventId,
                                     events:  NonEmptyList[CommitEvent]): IO[EventSchedulingResult] =
    semaphore.available flatMap {
      case 0 => Busy.pure[IO]
      case _ => {
        for {
          _ <- semaphore.acquire
          _ <- process(eventId, events).start
        } yield Accepted: EventSchedulingResult
      } recoverWith releasingSemaphore
    }

  private def process(eventId: CompoundEventId, events: NonEmptyList[CommitEvent]) = {
    for {
      _ <- eventProcessor.process(eventId, events)
      _ <- semaphore.release
    } yield ()
  } recoverWith {
    case NonFatal(exception) =>
      for {
        _ <- semaphore.release
        _ <- logger.error(exception)(s"Processing event $eventId failed")
      } yield ()
  }

  private def releasingSemaphore[O]: PartialFunction[Throwable, IO[O]] = {
    case NonFatal(exception) =>
      semaphore.release flatMap { _ =>
        exception.raiseError[IO, O]
      }
  }
}

private object IOEventsProcessingRunner {

  import ConfigLoader.find
  import eu.timepit.refined.pureconfig._

  import scala.language.postfixOps

  def apply(
      eventProcessor:      EventProcessor[IO],
      logger:              Logger[IO],
      config:              Config = ConfigFactory.load()
  )(implicit contextShift: ContextShift[IO]): IO[EventsProcessingRunner[IO]] =
    for {
      generationProcesses <- find[IO, Long Refined Positive]("generation-processes-number", config)
      semaphore           <- Semaphore(generationProcesses.value)
    } yield new IOEventsProcessingRunner(eventProcessor, semaphore, logger)
}
