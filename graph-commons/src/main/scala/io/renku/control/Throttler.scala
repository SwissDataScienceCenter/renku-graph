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

package io.renku.control

import cats.MonadThrow
import cats.effect.kernel.Clock
import cats.effect.std.Semaphore
import cats.effect.{Concurrent, Ref, Temporal}
import cats.syntax.all._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

trait Throttler[F[_], +ThrottlingTarget] {
  def acquire(): F[Unit]
  def release(): F[Unit]
}

final class StandardThrottler[F[_]: MonadThrow: Temporal: Clock, ThrottlingTarget] private[control] (
    rateLimit:         RateLimit[ThrottlingTarget],
    semaphore:         Semaphore[F],
    workersStartTimes: Ref[F, List[Long]]
) extends Throttler[F, ThrottlingTarget] {

  private val MinTimeGap       = (rateLimit.per.multiplierFor(NANOSECONDS) / rateLimit.items.value).toLong
  private val NextAttemptSleep = FiniteDuration(MinTimeGap / 10, TimeUnit.NANOSECONDS)

  override def acquire(): F[Unit] = for {
    _          <- semaphore.acquire
    startTimes <- workersStartTimes.get
    now        <- Clock[F].monotonic
    _          <- verifyThroughput(startTimes, now.toNanos)
  } yield ()

  private def verifyThroughput(startTimes: List[Long], now: Long) =
    if (notTooEarly(startTimes, now)) for {
      _ <- workersStartTimes.modify(old => (startTimes :+ now) -> old)
      _ <- semaphore.release
    } yield ()
    else
      for {
        _ <- semaphore.release
        _ <- Temporal[F] sleep NextAttemptSleep
        _ <- acquire()
      } yield ()

  private def notTooEarly(startTimes: List[Long], now: Long): Boolean = {
    val (_, durations) = (startTimes.tail :+ now).foldLeft(startTimes.head -> List.empty[Long]) {
      case ((previous, durationsSoFar), current) =>
        current -> (durationsSoFar :+ (current - previous))
    }

    durations.forall(_ >= MinTimeGap)
  }

  override def release(): F[Unit] = for {
    _ <- semaphore.acquire
    _ <- workersStartTimes.modify(old => old.tail -> old)
    _ <- semaphore.release
  } yield ()
}

object Throttler {

  def apply[F[_]: Concurrent: Temporal: Clock, ThrottlingTarget](
      rateLimit: RateLimit[ThrottlingTarget]
  ): F[Throttler[F, ThrottlingTarget]] = for {
    semaphore         <- Semaphore[F](1)
    workersStartTimes <- Clock[F].monotonic flatMap (now => Ref.of(List(now.toNanos)))
  } yield new StandardThrottler[F, ThrottlingTarget](rateLimit, semaphore, workersStartTimes)

  def noThrottling[F[_]: MonadThrow]: Throttler[F, Nothing] =
    new Throttler[F, Nothing] {
      override def acquire(): F[Unit] = ().pure[F]
      override def release(): F[Unit] = ().pure[F]
    }
}
