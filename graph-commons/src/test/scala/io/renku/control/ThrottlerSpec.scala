/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.effect._
import cats.effect.std.CountDownLatch
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.control.RateLimitUnit._
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class ThrottlerSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "throttle" should {

    "enforce processing with throughput not greater than demanded" in new TestCase {

      val tasksNumber = 20
      val rate        = RateLimit[ThrottlingTarget](10L, per = Second)

      val startTime = {
        for {
          throttler <- Throttler[IO, ThrottlingTarget](rate)
          startTime <- Clock[IO].monotonic
          latch     <- processConcurrently(tasksNumber, use = throttler)
          _         <- latch.await
        } yield startTime
      }.unsafeRunSync()

      val avgDelay = tasksStartDelays(startTime.toMillis).tail.sum / (tasksNumber - 1)
      avgDelay should be >= 100L
      avgDelay should be <= 300L

      totalTasksStartDelay(startTime.toMillis) should be > (tasksNumber * 100L)
    }

    "not sequence work items but process them in parallel" in new TestCase {

      val tasksNumber        = 20
      val rate               = RateLimit[ThrottlingTarget](200L, per = Second)
      val taskProcessingTime = 500 millis

      val startTime = {
        for {
          throttler <- Throttler[IO, ThrottlingTarget](rate)
          startTime <- Clock[IO].monotonic
          latch     <- processConcurrently(tasksNumber, use = throttler, taskProcessingTime = taskProcessingTime.some)
          _         <- latch.await
        } yield startTime
      }.unsafeRunSync()

      totalTasksStartDelay(startTime.toMillis) should be < (tasksNumber * taskProcessingTime.toMillis)
    }
  }

  "noThrottling" should {

    "return Throttler which does nothing to the throughput" in new TestCase {

      val tasksNumber = 20

      val startTime = {
        for {
          startTime <- Clock[IO].monotonic
          latch     <- processConcurrently(tasksNumber, use = Throttler.noThrottling[IO])
          _         <- latch.await
        } yield startTime
      }.unsafeRunSync()

      val (slowEvents, fastEvents) = tasksStartDelays(startTime.toMillis).partition(_ > 100L)
      slowEvents.size should be < 2
      fastEvents foreach { delay => delay should be < 100L }
      // it's actually never close to 100ms but it looks like CI is sometimes slooow
      // and there might be events that need more, especially when the test is warming up
    }
  }

  private trait TestCase {

    private val register = Ref.unsafe[IO, List[Long]](List.empty)

    def processConcurrently[ThrottlingTarget](tasks:              Int,
                                              use:                Throttler[IO, ThrottlingTarget],
                                              taskProcessingTime: Option[FiniteDuration] = None
    ): IO[CountDownLatch[IO]] =
      CountDownLatch[IO](tasks)
        .flatTap { latch =>
          (1 to tasks).toList.map(_ => useThrottledResource(use, latch, taskProcessingTime)).parSequence
        }

    private def useThrottledResource[Target](throttler:          Throttler[IO, Target],
                                             latch:              CountDownLatch[IO],
                                             taskProcessingTime: Option[FiniteDuration]
    ): IO[Unit] =
      throttler.throttle {
        noteProcessingStartTime >>
          latch.release >>
          taskProcessingTime.map(Temporal[IO].sleep).getOrElse(IO.unit)
      }

    private def noteProcessingStartTime =
      Clock[IO].monotonic >>= (scheduledAt => register.update(times => scheduledAt.toMillis :: times))

    def tasksStartDelays(startTime: Long): Seq[Long] =
      register.get
        .map(
          _.reverse.foldLeft(List.empty[Long], startTime) { case ((diffs, previousStart), item) =>
            (item - previousStart :: diffs) -> item
          }
        )
        .map(_._1)
        .unsafeRunSync()
        .reverse

    def totalTasksStartDelay(startTime: Long): Long =
      register.get
        .map(_.map(processingStartTime => processingStartTime - startTime).sum)
        .unsafeRunSync()
  }

  private trait ThrottlingTarget
}
