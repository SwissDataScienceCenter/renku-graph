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

import java.util.concurrent.ConcurrentHashMap

import cats.MonadError
import cats.effect._
import cats.implicits._
import ch.datascience.control.RateLimitUnit._
import eu.timepit.refined.auto._
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class ThrottlerSpec extends AnyWordSpec with should.Matchers {

  "Throttler" should {

    "enforce processing with throughput not greater than demanded" in new TestCase {

      val tasksNumber = 20

      val startTime = {
        for {
          throttler <- Throttler[IO, ThrottlingTarget](RateLimit(10L, per = Second))
          startTime <- clock.monotonic(MILLISECONDS)
          _         <- processConcurrently(tasksNumber, use = throttler)
        } yield startTime
      }.unsafeRunSync()

      val startDelays = tasksStartDelays(startTime)
      startDelays.sum / startDelays.size should be >= 100L

      totalTasksProcessingTime(startTime) should be > (tasksNumber * 100L)
    }

    "not sequence work items but process them in parallel" in new TestCase {

      val tasksNumber = 20

      val startTime = {
        for {
          throttler <- Throttler[IO, ThrottlingTarget](RateLimit(200L, per = Second))
          startTime <- clock.monotonic(MILLISECONDS)
          _         <- processConcurrently(tasksNumber, use = throttler, taskProcessingTime = Some(1000 millis))
        } yield startTime
      }.unsafeRunSync()

      totalTasksProcessingTime(startTime) should be < (tasksNumber * 1000L)
    }
  }

  "noThrottling" should {

    "return Throttler which does not do anything to the throughput" in new TestCase {

      val tasksNumber = 20

      val startTime = {
        for {
          startTime <- clock.monotonic(MILLISECONDS)
          _         <- processConcurrently(tasksNumber, use = Throttler.noThrottling[IO, Any])
        } yield startTime
      }.unsafeRunSync()

      tasksStartDelays(startTime) foreach { delay =>
        delay should be < 1000L
      }

      totalTasksProcessingTime(startTime) should be < (5 * 500L)
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)
  private val clock:                 Clock[IO]        = timer.clock

  private trait TestCase {

    val context  = MonadError[IO, Throwable]
    val register = new ConcurrentHashMap[String, Long]()

    def processConcurrently[ThrottlingTarget](tasks:              Int,
                                              use:                Throttler[IO, ThrottlingTarget],
                                              taskProcessingTime: Option[FiniteDuration] = None) =
      ((1 to tasks) map (useThrottledResource(_, use, taskProcessingTime))).toList.parSequence

    private def useThrottledResource[Target](name:               Int,
                                             throttler:          Throttler[IO, Target],
                                             taskProcessingTime: Option[FiniteDuration]): IO[Unit] =
      for {
        _          <- throttler.acquire
        greenLight <- clock.monotonic(MILLISECONDS)
        _          <- context.pure(register.put(name.toString, greenLight))
        _          <- taskProcessingTime.map(timer.sleep) getOrElse IO.unit
        _          <- throttler.release
      } yield ()

    def tasksStartDelays(startTime: Long) =
      register.asScala.values
        .map(greenLight => greenLight - startTime)
        .toList
        .sorted
        .foldLeft(List.empty[Long]) {
          case (diffs, item) => diffs :+ item - diffs.sum
        }

    def totalTasksProcessingTime(startTime: Long) =
      register.asScala.values
        .map(greenLight => greenLight - startTime)
        .sum
  }

  private trait ThrottlingTarget
}
