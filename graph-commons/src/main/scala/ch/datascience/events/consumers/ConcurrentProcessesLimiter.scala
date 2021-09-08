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

package ch.datascience.events.consumers

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, Busy, SchedulingError}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

import scala.util.control.NonFatal

trait ConcurrentProcessesLimiter[Interpretation[_]] {

  def tryExecuting(scheduledProcess: EventHandlingProcess[Interpretation]): Interpretation[EventSchedulingResult]
}

object ConcurrentProcessesLimiter {

  def apply(
      processesCount:    Int Refined Positive
  )(implicit concurrent: Concurrent[IO], contextShift: ContextShift[IO]): IO[ConcurrentProcessesLimiter[IO]] = for {
    semaphore <- Semaphore(processesCount.value)
  } yield new ConcurrentProcessesLimiterImpl[IO](processesCount, semaphore)

  def withoutLimit[Interpretation[_]: MonadThrow: Concurrent]: ConcurrentProcessesLimiter[Interpretation] =
    new ConcurrentProcessesLimiter[Interpretation] {
      override def tryExecuting(
          scheduledProcess: EventHandlingProcess[Interpretation]
      ): Interpretation[EventSchedulingResult] =
        scheduledProcess.process
          .widen[EventSchedulingResult]
          .merge
          .flatTap(_ => releaseIfNecessary(scheduledProcess.maybeReleaseProcess)) recoverWith { case NonFatal(error) =>
          releaseIfNecessary(scheduledProcess.maybeReleaseProcess).map(_ =>
            EventSchedulingResult.SchedulingError(error)
          )
        }

      private def releaseIfNecessary(maybeReleaseProcess: Option[Interpretation[Unit]]): Interpretation[Unit] =
        maybeReleaseProcess
          .map(Concurrent[Interpretation].start(_).void)
          .getOrElse(().pure[Interpretation])
          .recover { case NonFatal(_) => () }

    }
}

class ConcurrentProcessesLimiterImpl[Interpretation[_]: MonadThrow: ContextShift: Concurrent](
    processesCount: Int Refined Positive,
    semaphore:      Semaphore[Interpretation]
) extends ConcurrentProcessesLimiter[Interpretation] {

  def tryExecuting(
      process: EventHandlingProcess[Interpretation]
  ): Interpretation[EventSchedulingResult] =
    semaphore.available >>= {
      case 0 => Busy.pure[Interpretation].widen[EventSchedulingResult]
      case _ =>
        {
          for {
            _      <- semaphore.acquire
            result <- startProcess(process)
          } yield result
        } recoverWith releasingSemaphore
    }

  private def startProcess(process: EventHandlingProcess[Interpretation]): Interpretation[EventSchedulingResult] = {
    process.process.semiflatTap { _ =>
      Concurrent[Interpretation].start(waitForRelease(process))
    } recoverWith releaseOnLeft
  }.widen[EventSchedulingResult].merge recoverWith releaseOnError

  private def waitForRelease(process: EventHandlingProcess[Interpretation]) =
    process.waitToFinish() >> releaseAndNotify(process).void

  private lazy val releaseOnLeft
      : PartialFunction[EventSchedulingResult, EitherT[Interpretation, EventSchedulingResult, Accepted]] = {
    case eventSchedulingResult =>
      EitherT.left(semaphore.release.map(_ => eventSchedulingResult))
  }

  private lazy val releaseOnError: PartialFunction[Throwable, Interpretation[EventSchedulingResult]] = {
    case NonFatal(error) => semaphore.release.map(_ => SchedulingError(error))
  }

  private def releaseAndNotify(scheduledProcess: EventHandlingProcess[Interpretation]): Interpretation[Unit] =
    for {
      _ <- semaphore.release
      _ <- scheduledProcess.maybeReleaseProcess
             .map(Concurrent[Interpretation].start(_).void)
             .getOrElse(().pure[Interpretation])
             .recover { case NonFatal(_) =>
               ()
             }
    } yield ()

  private def releasingSemaphore[O]: PartialFunction[Throwable, Interpretation[O]] = { case NonFatal(exception) =>
    semaphore.available flatMap {
      case available if available == processesCount.value => exception.raiseError[Interpretation, O]
      case _                                              => semaphore.release flatMap { _ => exception.raiseError[Interpretation, O] }
    }
  }
}
