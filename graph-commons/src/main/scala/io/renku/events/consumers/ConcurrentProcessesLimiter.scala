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

package io.renku.events.consumers

import cats.data.EitherT
import cats.effect.std.Semaphore
import cats.effect.{Concurrent, Spawn}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.renku.events.consumers.EventSchedulingResult.{Accepted, Busy, SchedulingError}

import scala.util.control.NonFatal

trait ConcurrentProcessesLimiter[F[_]] {
  def tryExecuting(scheduledProcess: EventHandlingProcess[F]): F[EventSchedulingResult]
}

object ConcurrentProcessesLimiter {

  def apply[F[_]: Concurrent](
      processesCount: Int Refined Positive
  ): F[ConcurrentProcessesLimiter[F]] = for {
    semaphore <- Semaphore(processesCount.value)
  } yield new ConcurrentProcessesLimiterImpl[F](processesCount, semaphore)

  def withoutLimit[F[_]: Concurrent]: ConcurrentProcessesLimiter[F] =
    new ConcurrentProcessesLimiter[F] {

      override def tryExecuting(scheduledProcess: EventHandlingProcess[F]): F[EventSchedulingResult] =
        scheduledProcess.process
          .widen[EventSchedulingResult]
          .merge
          .flatTap(_ => releaseIfNecessary(scheduledProcess.maybeReleaseProcess)) recoverWith { case NonFatal(error) =>
          releaseIfNecessary(scheduledProcess.maybeReleaseProcess).map(_ =>
            EventSchedulingResult.SchedulingError(error)
          )
        }

      private def releaseIfNecessary(maybeReleaseProcess: Option[F[Unit]]): F[Unit] =
        maybeReleaseProcess
          .map(Concurrent[F].start(_).void)
          .getOrElse(().pure[F])
          .recover { case NonFatal(_) => () }

    }
}

class ConcurrentProcessesLimiterImpl[F[_]: Concurrent](
    processesCount: Int Refined Positive,
    semaphore:      Semaphore[F]
) extends ConcurrentProcessesLimiter[F] {

  def tryExecuting(
      process: EventHandlingProcess[F]
  ): F[EventSchedulingResult] = semaphore.available >>= {
    case 0 => Busy.pure[F].widen[EventSchedulingResult]
    case _ =>
      {
        for {
          _      <- semaphore.acquire
          result <- startProcess(process)
        } yield result
      } recoverWith releasingSemaphore
  }

  private def startProcess(process: EventHandlingProcess[F]): F[EventSchedulingResult] = {
    process.process.semiflatTap { _ =>
      Concurrent[F].start(waitForRelease(process))
    } recoverWith releaseOnLeft
  }.widen[EventSchedulingResult].merge recoverWith releaseOnError

  private def waitForRelease(process: EventHandlingProcess[F]) =
    process.waitToFinish() >> releaseAndNotify(process).void

  private lazy val releaseOnLeft
      : PartialFunction[EventSchedulingResult, EitherT[F, EventSchedulingResult, Accepted]] = {
    case eventSchedulingResult =>
      EitherT.left(semaphore.release.map(_ => eventSchedulingResult))
  }

  private lazy val releaseOnError: PartialFunction[Throwable, F[EventSchedulingResult]] = { case NonFatal(error) =>
    semaphore.release.map(_ => SchedulingError(error))
  }

  private def releaseAndNotify(scheduledProcess: EventHandlingProcess[F]): F[Unit] =
    for {
      _ <- semaphore.release
      _ <- scheduledProcess.maybeReleaseProcess
             .map(Spawn[F].start(_).void)
             .getOrElse(().pure[F])
             .recover { case NonFatal(_) =>
               ()
             }
    } yield ()

  private def releasingSemaphore[O]: PartialFunction[Throwable, F[O]] = { case NonFatal(exception) =>
    semaphore.available flatMap {
      case available if available == processesCount.value => exception.raiseError[F, O]
      case _ => semaphore.release flatMap { _ => exception.raiseError[F, O] }
    }
  }
}
