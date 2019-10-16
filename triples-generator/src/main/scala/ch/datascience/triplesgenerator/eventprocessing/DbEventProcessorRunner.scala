/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.effect.IO._
import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import ch.datascience.config.ConfigLoader
import ch.datascience.dbeventlog.commands.EventLogFetch
import ch.datascience.logging.ApplicationLogger
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class DbEventProcessorRunner private (
    eventProcessor:      EventProcessor[IO],
    eventLogFetch:       EventLogFetch[IO],
    semaphore:           Semaphore[IO],
    logger:              Logger[IO]
)(implicit contextShift: ContextShift[IO], executionContext: ExecutionContext, timer: Timer[IO])
    extends EventProcessorRunner[IO](eventProcessor) {

  import DbEventProcessorRunner._
  import eventLogFetch._

  lazy val run: IO[Unit] = for {
    _ <- logger.info("Waiting for new events")
    _ <- checkProcessesNumber
  } yield ()

  private def checkProcessesNumber: IO[Unit] =
    semaphore.available flatMap {
      case 0 => (timer sleep maxProcessesSleep) *> checkProcessesNumber
      case _ => checkForNewEvent
    }

  private def checkForNewEvent: IO[Unit] =
    isEventToProcess flatMap {
      case true  => List(popEvent, checkProcessesNumber).parSequence *> IO.unit
      case false => (timer sleep noEventsSleep) *> checkProcessesNumber
    }

  private def popEvent: IO[Unit] =
    for {
      _ <- semaphore.acquire
      _ <- popEventToProcess flatMap {
            case Some(eventBody) => eventProcessor(eventBody) recoverWith { case _ => IO.unit }
            case None            => IO.unit
          }
      _ <- semaphore.release
    } yield ()
}

object DbEventProcessorRunner extends ConfigLoader[IO] {
  import eu.timepit.refined.pureconfig._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  def apply(
      eventProcessor:      EventProcessor[IO],
      eventLogFetch:       EventLogFetch[IO],
      config:              Config = ConfigFactory.load(),
      logger:              Logger[IO] = ApplicationLogger
  )(implicit contextShift: ContextShift[IO],
    executionContext:      ExecutionContext,
    timer:                 Timer[IO]): IO[DbEventProcessorRunner] =
    for {
      generationProcesses <- find[Long Refined Positive]("generation-processes-number", config)
      semaphore           <- Semaphore(generationProcesses.value)
    } yield new DbEventProcessorRunner(eventProcessor, eventLogFetch, semaphore, logger)

  private val noEventsSleep:     FiniteDuration = 2 seconds
  private val maxProcessesSleep: FiniteDuration = 50 millis
}
