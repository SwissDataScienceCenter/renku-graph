/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect._
import cats.effect.kernel.Clock
import cats.effect.std.Semaphore
import cats.syntax.all._

import scala.concurrent.duration._

trait Throttler[F[_], +ThrottlingTarget] {
  def throttle[O](value: F[O]): F[O]
}

final class StandardThrottler[F[_]: MonadThrow: Async: Clock, ThrottlingTarget] private[control] (
    rateLimit: RateLimit[ThrottlingTarget],
    semaphore: Semaphore[F]
) extends Throttler[F, ThrottlingTarget] {

  private val MinTimeGap        = (rateLimit.per.multiplierFor(NANOSECONDS) / rateLimit.items.value).toLong
  private val NextAttemptSleep  = FiniteDuration(MinTimeGap / 10, NANOSECONDS)
  private val previousStartedAt = Ref.unsafe[F, Long](0L)

  override def throttle[O](value: F[O]): F[O] =
    waitIfTooFast >> value

  private def waitIfTooFast: F[Unit] =
    semaphore.tryPermit.use[Unit] {
      case true =>
        waitIfTooEarlyForNext >>
          updateStartedAt()
      case false =>
        Temporal[F].delayBy(waitIfTooFast, NextAttemptSleep)
    }

  private def waitIfTooEarlyForNext: F[Unit] =
    (previousStartedAt.get -> now)
      .flatMapN {
        case (previousStartedAt, now) if (now - previousStartedAt) >= MinTimeGap => ().pure[F]
        case _ => Temporal[F].delayBy(waitIfTooEarlyForNext, NextAttemptSleep)
      }

  private def updateStartedAt() =
    now >>= previousStartedAt.set

  private def now = Clock[F].monotonic.map(_.toNanos)
}

object Throttler {

  def apply[F[_]: Concurrent: Async: Clock, ThrottlingTarget](
      rateLimit: RateLimit[ThrottlingTarget]
  ): F[Throttler[F, ThrottlingTarget]] =
    Semaphore[F](1).map(new StandardThrottler[F, ThrottlingTarget](rateLimit, _))

  def noThrottling[F[_]: MonadThrow]: Throttler[F, Nothing] =
    new Throttler[F, Nothing] {
      override def throttle[O](value: F[O]): F[O] = value
    }
}
