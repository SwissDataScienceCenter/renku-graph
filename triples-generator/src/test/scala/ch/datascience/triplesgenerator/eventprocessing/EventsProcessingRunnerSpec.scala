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

package ch.datascience.triplesgenerator.eventprocessing

import java.lang.Thread.sleep

import EventProcessingGenerators._
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.triplesgenerator.eventprocessing.EventsProcessingRunner.EventSchedulingResult.{Accepted, Busy}
import ch.datascience.triplesgenerator.subscriptions.Subscriber
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class EventsProcessingRunnerSpec
    extends AnyWordSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  "scheduleForProcessing" should {

    s"return $Accepted if there is enough capacity to process an event" in new TestCase {
      processingRunner.scheduleForProcessing(eventId, events).unsafeRunSync() shouldBe Accepted
    }

    s"return $Busy when processing capacity is reached " +
      s"and $Accepted once some of the scheduled events are done" in new TestCase {

      // draining processing capacity by scheduling max number of jobs
      (1 to processesNumber).toList map { _ =>
        processingRunner.scheduleForProcessing(eventId, events).unsafeRunSync()
      }

      // any new job to get the Busy status
      processingRunner.scheduleForProcessing(eventId, events).unsafeRunSync() shouldBe Busy

      expectAvailabilityIsCommunicated

      // once at least one process is done, new events should be accepted again
      sleep(eventProcessingTime + 50)
      processingRunner.scheduleForProcessing(eventId, events).unsafeRunSync() shouldBe Accepted
    }

    "release the processing resource on processing failure" in new TestCase {

      // draining processing capacity by scheduling max number of jobs
      processingRunner.scheduleForProcessing(eventIdCausingFailure, events).unsafeRunSync()
      processingRunner.scheduleForProcessing(eventIdCausingFailure, events).unsafeRunSync()

      // any new job to get the Busy status
      processingRunner.scheduleForProcessing(eventId, events).unsafeRunSync() shouldBe Busy

      expectAvailabilityIsCommunicated

      // once at least one process is done, new events should be accepted again
      sleep(eventProcessingTime + 50)
      processingRunner.scheduleForProcessing(eventId, events).unsafeRunSync() shouldBe Accepted

      eventually {
        logger.logged(Error(s"Processing event $eventIdCausingFailure failed", exception))
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestCase {

    val eventId               = compoundEventIds.generateOne
    val eventIdCausingFailure = compoundEventIds.generateOne
    val events                = commitEvents.generateNonEmptyList()
    val exception             = exceptions.generateOne

    val eventProcessingTime = 500
    val eventProcessor: EventProcessor[IO] =
      (id: CompoundEventId, _: NonEmptyList[CommitEvent]) =>
        id match {
          case `eventIdCausingFailure` =>
            timer sleep (eventProcessingTime millis) flatMap (_ => exception.raiseError[IO, Unit])
          case _ =>
            timer sleep (eventProcessingTime millis)
        }

    val processesNumber = 2
    private val config = ConfigFactory.parseMap(
      Map(
        "generation-processes-number" -> processesNumber
      ).asJava
    )

    val logger           = TestLogger[IO]()
    val subscriber       = mock[Subscriber[IO]]
    val processingRunner = IOEventsProcessingRunner(eventProcessor, subscriber, logger, config).unsafeRunSync()

    def expectAvailabilityIsCommunicated =
      (subscriber.notifyAvailability _)
        .expects()
        .returning(IO.unit)
        .atLeastOnce()
  }
}
