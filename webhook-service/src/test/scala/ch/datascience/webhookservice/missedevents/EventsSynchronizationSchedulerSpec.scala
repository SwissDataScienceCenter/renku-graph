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

package ch.datascience.webhookservice.missedevents

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.{postfixOps, reflectiveCalls}

class EventsSynchronizationSchedulerSpec
    extends AnyWordSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  "run" should {

    "kick off the events synchronization process " +
      "with initial delay read from config and " +
      "continues endlessly with interval periods" in new TestCase {

        scheduler.run().start.unsafeRunAsyncAndForget()

        eventually {
          eventsLoader.callCounter.get() should be > 5
        }
      }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestCase {

    val eventsLoader = new MissedEventsLoader[IO] {
      val callCounter = new AtomicInteger(0)

      override def loadMissedEvents: IO[Unit] = {
        Thread.sleep(5)
        callCounter.incrementAndGet()
        if (callCounter.get() == 2) exceptions.generateOne.raiseError[IO, Unit]
        else ().pure[IO]
      }
    }

    val configProvider = mock[IOSchedulerConfigProvider]
    val scheduler      = new EventsSynchronizationScheduler[IO](configProvider, eventsLoader, TestLogger())

    (configProvider.getInitialDelay _)
      .expects()
      .returning((100 milliseconds).pure[IO])
    (configProvider.getInterval _)
      .expects()
      .returning((500 milliseconds).pure[IO])
  }

  private class IOSchedulerConfigProvider extends SchedulerConfigProvider[IO]
}
