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

package io.renku.control

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.control.RateLimitUnit._
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class ThrottlerSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "Throttler" should {

    "enforce processing with throughput not greater than demanded" in new TestCase {

      val tasksNumber = 20

      val startTime = {
        for {
          throttler <- Throttler[IO, ThrottlingTarget](RateLimit(10L, per = Second))
          startTime <- Clock[IO].monotonic
          _         <- processConcurrently(tasksNumber, use = throttler)
        } yield startTime
      }.unsafeRunSync()

      val startDelays = tasksStartDelays(startTime.toMillis)
      startDelays.sum / startDelays.size should be >= 100L

      totalTasksStartDelay(startTime.toMillis) should be > (tasksNumber * 100L)
    }

    "not sequence work items but process them in parallel" in new TestCase {

      val tasksNumber = 20

      val startTime = {
        for {
          throttler <- Throttler[IO, ThrottlingTarget](RateLimit(200L, per = Second))
          startTime <- Clock[IO].monotonic
          _         <- processConcurrently(tasksNumber, use = throttler, taskProcessingTime = Some(1000 millis))
        } yield startTime
      }.unsafeRunSync()

      totalTasksStartDelay(startTime.toMillis) should be < (tasksNumber * 1000L)
    }
  }

  "noThrottling" should {

    "return Throttler which does nothing to the throughput" in new TestCase {

      val tasksNumber = 20

      val startTime = {
        for {
          startTime <- Clock[IO].monotonic
          _         <- processConcurrently(tasksNumber, use = Throttler.noThrottling[IO])
        } yield startTime
      }.unsafeRunSync()

      tasksStartDelays(startTime.toMillis) foreach { delay => delay should be < 100L }
    }
  }

  private trait TestCase {

    val register = new ConcurrentHashMap[String, Long]()

    def processConcurrently[ThrottlingTarget](tasks:              Int,
                                              use:                Throttler[IO, ThrottlingTarget],
                                              taskProcessingTime: Option[FiniteDuration] = None
    ) = ((1 to tasks) map (useThrottledResource(_, use, taskProcessingTime))).toList.parSequence

    private def useThrottledResource[Target](name:               Int,
                                             throttler:          Throttler[IO, Target],
                                             taskProcessingTime: Option[FiniteDuration]
    ): IO[Unit] = for {
      _          <- throttler.acquire()
      greenLight <- Clock[IO].monotonic
      _          <- register.put(name.toString, greenLight.toMillis).pure[IO]
      _          <- taskProcessingTime.map(Temporal[IO].sleep) getOrElse IO.unit
      _          <- throttler.release()
    } yield ()

    def tasksStartDelays(startTime: Long): Seq[Long] =
      register.asScala.values
        .map(greenLight => greenLight - startTime)
        .toList
        .sorted
        .foldLeft(List.empty[Long]) { case (diffs, item) =>
          diffs :+ item - diffs.sum
        }

    def totalTasksStartDelay(startTime: Long): Long =
      register.asScala.values
        .map(greenLight => greenLight - startTime)
        .sum
  }

  private trait ThrottlingTarget
}
