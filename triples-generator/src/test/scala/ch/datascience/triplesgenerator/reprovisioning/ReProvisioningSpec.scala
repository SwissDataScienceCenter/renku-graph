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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import cats.effect.{IO, Timer}
import cats.implicits._
import ch.datascience.dbeventlog.commands.IOEventLogReScheduler
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.logging.TestExecutionTimeRecorder
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class ReProvisioningSpec extends WordSpec with MockFactory {

  "run" should {

    "clear the RDF Store and reschedule for processing all Commit Events in the Log if store outdated" in new TestCase {
      inSequence {
        (triplesVersionFinder.triplesUpToDate _)
          .expects()
          .returning(context.pure(false))

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(context.unit)

        (eventLogReScheduler.scheduleEventsForProcessing _)
          .expects()
          .returning(context.unit)
      }

      reProvisioning.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info(s"ReProvisioning triggered in ${executionTimeRecorder.elapsedTime}ms"))
    }

    "do nothing if there all RDF store is up to date" in new TestCase {
      (triplesVersionFinder.triplesUpToDate _)
        .expects()
        .returning(context.pure(true))

      reProvisioning.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("All projects' triples up to date"))
    }

    "do not fail but simply retry if checking if RDF store is up to date fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {
        (triplesVersionFinder.triplesUpToDate _)
          .expects()
          .returning(context.raiseError(exception))

        (triplesVersionFinder.triplesUpToDate _)
          .expects()
          .returning(context.pure(true))
      }

      reProvisioning.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Error("Re-provisioning failure", exception),
        Info("All projects' triples up to date")
      )
    }

    "do not fail but simply retry if removing outdated triples fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {
        (triplesVersionFinder.triplesUpToDate _)
          .expects()
          .returning(context.pure(false))

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(context.raiseError(exception))

        (triplesVersionFinder.triplesUpToDate _)
          .expects()
          .returning(context.pure(false))

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(context.unit)

        (eventLogReScheduler.scheduleEventsForProcessing _)
          .expects()
          .returning(context.unit)
      }

      reProvisioning.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Error("Re-provisioning failure", exception),
        Info(s"ReProvisioning triggered in ${executionTimeRecorder.elapsedTime}ms")
      )
    }

    "do not fail but simply retry if re-scheduling events fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {
        (triplesVersionFinder.triplesUpToDate _)
          .expects()
          .returning(context.pure(false))

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(context.unit)

        (eventLogReScheduler.scheduleEventsForProcessing _)
          .expects()
          .returning(context.raiseError(exception))

        (triplesVersionFinder.triplesUpToDate _)
          .expects()
          .returning(context.pure(false))

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(context.unit)

        (eventLogReScheduler.scheduleEventsForProcessing _)
          .expects()
          .returning(context.unit)
      }

      reProvisioning.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Error("Re-provisioning failure", exception),
        Info(s"ReProvisioning triggered in ${executionTimeRecorder.elapsedTime}ms")
      )
    }

    "start re-provisioning after the initial delay" in new TestCase {
      (triplesVersionFinder.triplesUpToDate _)
        .expects()
        .returning(context.pure(true))

      val someInitialDelay: ReProvisioningDelay = ReProvisioningDelay(500 millis)

      val startTime = System.currentTimeMillis()

      new ReProvisioning[IO](
        triplesVersionFinder,
        triplesRemover,
        eventLogReScheduler,
        someInitialDelay,
        executionTimeRecorder,
        logger,
        5 millis
      ).run.unsafeRunSync() shouldBe ((): Unit)

      val endTime = System.currentTimeMillis()

      (endTime - startTime) should be >= someInitialDelay.value.toMillis
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val triplesVersionFinder  = mock[TriplesVersionFinder[IO]]
    val triplesRemover        = mock[TriplesRemover[IO]]
    val eventLogReScheduler   = mock[IOEventLogReScheduler]
    val initialDelay          = ReProvisioningDelay(durations(100 millis).generateOne)
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder(logger)
    val reProvisioning = new ReProvisioning[IO](
      triplesVersionFinder,
      triplesRemover,
      eventLogReScheduler,
      initialDelay,
      executionTimeRecorder,
      logger,
      5 millis
    )
  }
}
