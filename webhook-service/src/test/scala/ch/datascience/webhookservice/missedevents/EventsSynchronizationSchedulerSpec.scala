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

package ch.datascience.webhookservice.missedevents

import java.util.concurrent.atomic.AtomicInteger

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.{postfixOps, reflectiveCalls}
import scala.util.Try

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

      (timer
        .sleep(_: FiniteDuration))
        .expects(5 minutes)
        .returning(context.unit)

      (timer
        .sleep(_: FiniteDuration))
        .expects(1 hour)
        .returning(context.unit)
        .atLeastOnce()

      IO.suspend(scheduler.run.pure[IO]).start.unsafeRunAsyncAndForget()

      eventually {
        eventsLoader.callCounter.get() should be > 5
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val eventsLoader = new MissedEventsLoader[Try] {
      val callCounter = new AtomicInteger(0)

      override def loadMissedEvents = {
        Thread.sleep(5)
        callCounter.incrementAndGet()
        if (callCounter.get() == 5) context.raiseError(exceptions.generateOne)
        else context.unit
      }
    }

    val configProvider = mock[TrySchedulerConfigProvider]
    val timer          = mock[Timer[Try]]
    val scheduler      = new EventsSynchronizationScheduler[Try](configProvider, eventsLoader)(context, timer)

    (configProvider.getInitialDelay _)
      .expects()
      .returning(context.pure(5 minutes))
    (configProvider.getInterval _)
      .expects()
      .returning(context.pure(1 hour))
  }

  private class TrySchedulerConfigProvider extends SchedulerConfigProvider[Try]
}
