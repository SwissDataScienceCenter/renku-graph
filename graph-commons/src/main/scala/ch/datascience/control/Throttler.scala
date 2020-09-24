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

package ch.datascience.control

import java.util.concurrent.TimeUnit

import cats.MonadError
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, Timer}
import cats.syntax.all._

import scala.concurrent.duration._

trait Throttler[Interpretation[_], ThrottlingTarget] {
  def acquire(): Interpretation[Unit]

  def release(): Interpretation[Unit]
}

final class StandardThrottler[Interpretation[_], ThrottlingTarget] private[control] (
    rateLimit:         RateLimit[ThrottlingTarget],
    semaphore:         Semaphore[Interpretation],
    workersStartTimes: Ref[Interpretation, List[Long]]
)(implicit ME:         MonadError[Interpretation, Throwable], timer: Timer[Interpretation])
    extends Throttler[Interpretation, ThrottlingTarget] {

  private val MinTimeGap       = (rateLimit.per.multiplierFor(NANOSECONDS) / rateLimit.items.value).toLong
  private val NextAttemptSleep = FiniteDuration(MinTimeGap / 10, TimeUnit.NANOSECONDS)

  import timer.clock

  override def acquire(): Interpretation[Unit] =
    for {
      _          <- semaphore.acquire
      startTimes <- workersStartTimes.get
      now        <- clock.monotonic(NANOSECONDS)
      _          <- verifyThroughput(startTimes, now)
    } yield ()

  private def verifyThroughput(startTimes: List[Long], now: Long) =
    if (notTooEarly(startTimes, now)) for {
      _ <- workersStartTimes.modify(old => (startTimes :+ now) -> old)
      _ <- semaphore.release
    } yield ()
    else
      for {
        _ <- semaphore.release
        _ <- timer sleep NextAttemptSleep
        _ <- acquire()
      } yield ()

  private def notTooEarly(startTimes: List[Long], now: Long): Boolean = {
    val (_, durations) = (startTimes.tail :+ now).foldLeft(startTimes.head -> List.empty[Long]) {
      case ((previous, durationsSoFar), current) =>
        current -> (durationsSoFar :+ (current - previous))
    }

    durations.forall(_ >= MinTimeGap)
  }

  override def release(): Interpretation[Unit] =
    for {
      _ <- semaphore.acquire
      _ <- workersStartTimes.modify(old => old.tail -> old)
      _ <- semaphore.release
    } yield ()
}

object Throttler {

  def apply[Interpretation[_], ThrottlingTarget](
      rateLimit: RateLimit[ThrottlingTarget]
  )(implicit
      F:     Concurrent[Interpretation],
      timer: Timer[Interpretation]
  ): Interpretation[Throttler[Interpretation, ThrottlingTarget]] =
    for {
      semaphore         <- Semaphore[Interpretation](1)
      workersStartTimes <- timer.clock.monotonic(NANOSECONDS) flatMap (now => Ref.of(List(now)))
    } yield new StandardThrottler[Interpretation, ThrottlingTarget](rateLimit, semaphore, workersStartTimes)

  def noThrottling[Interpretation[_], ThrottlingTarget](implicit
      ME: MonadError[Interpretation, Throwable]
  ): Throttler[Interpretation, ThrottlingTarget] = new Throttler[Interpretation, ThrottlingTarget] {
    override def acquire(): Interpretation[Unit] = ME.unit

    override def release(): Interpretation[Unit] = ME.unit
  }
}
