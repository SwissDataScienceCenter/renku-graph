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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration

import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.syntax.all._
import ch.datascience.events.consumers.ConsumersModelGenerators.projects
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.EventProcessingGenerators._
import eu.timepit.refined.api.Refined
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class EventsProcessingRunnerSpec
    extends AnyWordSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(3, Seconds)),
    interval = scaled(Span(100, Millis))
  )

  "scheduleForProcessing" should {

    s"return $Accepted if there is enough capacity to process an event" in new TestCase {
      processingRunner.scheduleForProcessing(commitEvents.generateOne, schemaVersion).unsafeRunSync() shouldBe Accepted
    }

    s"return $Busy when processing capacity is reached " +
      s"and $Accepted once some of the scheduled events are done" in new TestCase {

        // draining processing capacity by scheduling max number of jobs
        commitEvents.generateFixedSizeList(Refined.unsafeApply(processesNumber.value)) map {
          processingRunner.scheduleForProcessing(_, schemaVersion).unsafeRunSync()
        }

        // any new job to get the Busy status
        processingRunner.scheduleForProcessing(commitEvents.generateOne, schemaVersion).unsafeRunSync() shouldBe Busy

        expectAvailabilityIsCommunicated

        // once at least one process is done, new events should be accepted again
        sleep(eventProcessingTime.toMillis + 250)
        processingRunner
          .scheduleForProcessing(commitEvents.generateOne, schemaVersion)
          .unsafeRunSync() shouldBe Accepted
      }

    "release the processing resource on processing failure" in new TestCase {

      // draining processing capacity by scheduling max number of jobs
      val failingEvent = commitEvents.generateOne.copy(
        eventId = eventIdCausingFailure.id,
        project = projects.generateOne.copy(id = eventIdCausingFailure.projectId)
      )
      processingRunner.scheduleForProcessing(failingEvent, schemaVersion).unsafeRunSync()
      processingRunner.scheduleForProcessing(commitEvents.generateOne, schemaVersion).unsafeRunSync()

      // any new job to get the Busy status
      processingRunner.scheduleForProcessing(commitEvents.generateOne, schemaVersion).unsafeRunSync() shouldBe Busy

      expectAvailabilityIsCommunicated

      // once at least one process is done, new events should be accepted again
      sleep(eventProcessingTime.toMillis + 250)
      processingRunner.scheduleForProcessing(commitEvents.generateOne, schemaVersion).unsafeRunSync() shouldBe Accepted

      eventually {
        logger.logged(Error(s"Processing event $eventIdCausingFailure failed", exception))
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestCase {

    val eventIdCausingFailure = compoundEventIds.generateOne
    val exception             = exceptions.generateOne
    val schemaVersion         = projectSchemaVersions.generateOne

    val eventProcessingTime = 500 millis
    val eventProcessor: EventProcessor[IO] = (event: CommitEvent, _: SchemaVersion) =>
      event.compoundEventId match {
        case `eventIdCausingFailure` =>
          timer sleep eventProcessingTime flatMap (_ => exception.raiseError[IO, Unit])
        case _ =>
          timer sleep eventProcessingTime
      }

    val processesNumber = GenerationProcessesNumber(2)
    val semaphore       = Semaphore[IO](processesNumber.value).unsafeRunSync()
    val logger          = TestLogger[IO]()
    val subscriber      = mock[SubscriptionMechanism[IO]]
    val processingRunner =
      new EventsProcessingRunnerImpl(eventProcessor, processesNumber, semaphore, subscriber, logger)

    def expectAvailabilityIsCommunicated =
      (subscriber.renewSubscription _)
        .expects()
        .returning(IO.unit)
        .atLeastOnce()
  }
}
