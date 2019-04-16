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

package ch.datascience.control

import cats.MonadError
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, Timer}
import cats.implicits._

import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}

trait Throttler[Interpretation[_], ThrottlingTarget] {
  def acquire: Interpretation[Unit]
  def release: Interpretation[Unit]
}

final class StandardThrottler[Interpretation[_], ThrottlingTarget](
    rateLimit:         RateLimit,
    semaphore:         Semaphore[Interpretation],
    workersStartTimes: Ref[Interpretation, List[BigDecimal]]
)(implicit ME:         MonadError[Interpretation, Throwable], timer: Timer[Interpretation])
    extends Throttler[Interpretation, ThrottlingTarget] {

  private val NextAttemptSleep: FiniteDuration = 100 millis
  private val MinTimeGap = BigDecimal(rateLimit.items.value) / BigDecimal(rateLimit.per.toMillis)

  import timer.clock

  override def acquire: Interpretation[Unit] =
    for {
      _          <- semaphore.acquire
      startTimes <- workersStartTimes.get
      now        <- clock.monotonic(MILLISECONDS) map (BigDecimal(_))
      _          <- verifyThroughput(startTimes, now)
    } yield ()

  private def verifyThroughput(startTimes: List[BigDecimal], now: BigDecimal) = {
    val oldest = startTimes.head
    if (BigDecimal(startTimes.size) / (now - oldest) <= MinTimeGap)
      workersStartTimes.modify(old => (startTimes :+ now) -> old) *> semaphore.release
    else semaphore.release *> timer.sleep(NextAttemptSleep) *> acquire
  }

  override def release: Interpretation[Unit] =
    for {
      _ <- semaphore.acquire
      _ <- workersStartTimes.modify(old => old.tail -> old)
      _ <- semaphore.release
    } yield ()
}

object Throttler {

  def apply[Interpretation[_], ThrottlingTarget](
      rateLimit: RateLimit
  )(implicit F:  Concurrent[Interpretation],
    timer:       Timer[Interpretation]): Interpretation[Throttler[Interpretation, ThrottlingTarget]] =
    for {
      semaphore         <- Semaphore[Interpretation](1)
      workersStartTimes <- timer.clock.monotonic(MILLISECONDS) flatMap (now => Ref.of(List(BigDecimal(now))))
    } yield new StandardThrottler[Interpretation, ThrottlingTarget](rateLimit, semaphore, workersStartTimes)

  def noThrottling[Interpretation[_], ThrottlingTarget](
      implicit ME: MonadError[Interpretation, Throwable]
  ): Throttler[Interpretation, ThrottlingTarget] = new Throttler[Interpretation, ThrottlingTarget] {
    override def acquire: Interpretation[Unit] = ME.unit
    override def release: Interpretation[Unit] = ME.unit
  }
}
