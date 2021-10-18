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

package io.renku.triplesgenerator.reprovisioning

import cats.effect.{IO, Timer}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.RenkuVersionPair
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class ReProvisioningSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    "clear the RDF Store and reschedule all Commit Events for processing in the Log if store is outdated" in new TestCase {

      inSequence {
        (renkuVersionPairFinder.find _)
          .expects()
          .returning(IO(maybeCurrentVersionCompatibilityPair))

        (reprovisionJudge.isReProvisioningNeeded _)
          .expects(maybeCurrentVersionCompatibilityPair, versionCompatibilityPairs)
          .returning(true)

        (reProvisioningStatus.setRunning _)
          .expects()
          .returning(IO.unit)

        (renkuVersionPairUpdater.update _)
          .expects(versionCompatibilityPairs.head)
          .returning(IO.unit)

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(IO.unit)

        (eventsReScheduler.triggerEventsReScheduling _)
          .expects()
          .returning(IO.unit)

        (reProvisioningStatus.clear _)
          .expects()
          .returning(IO.unit)
      }

      reProvisioning.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Info("The triples are not up to date - re-provisioning is clearing DB"),
        Info(s"Clearing DB finished in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do nothing if there all RDF store is up to date" in new TestCase {
      (renkuVersionPairFinder.find _)
        .expects()
        .returning(IO(maybeCurrentVersionCompatibilityPair))

      (reprovisionJudge.isReProvisioningNeeded _)
        .expects(maybeCurrentVersionCompatibilityPair, versionCompatibilityPairs)
        .returning(false)

      (renkuVersionPairUpdater.update _)
        .expects(versionCompatibilityPairs.head)
        .returning(IO.unit)

      reProvisioning.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("All projects' triples up to date"))
    }

    "do not fail but simply retry if checking if finding version pair in RDF store fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {
        (renkuVersionPairFinder.find _)
          .expects()
          .returning(exception.raiseError[IO, Option[RenkuVersionPair]])

        (renkuVersionPairFinder.find _)
          .expects()
          .returning(maybeCurrentVersionCompatibilityPair.pure[IO])

        (reprovisionJudge.isReProvisioningNeeded _)
          .expects(maybeCurrentVersionCompatibilityPair, versionCompatibilityPairs)
          .returning(false)

        (renkuVersionPairUpdater.update _)
          .expects(versionCompatibilityPairs.head)
          .returning(IO.unit)
      }

      reProvisioning.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Error("Re-provisioning failure", exception),
        Info("All projects' triples up to date")
      )
    }

    "do not fail but simply retry if setRunning method fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {
        (renkuVersionPairFinder.find _)
          .expects()
          .returning(maybeCurrentVersionCompatibilityPair.pure[IO])

        (reprovisionJudge.isReProvisioningNeeded _)
          .expects(maybeCurrentVersionCompatibilityPair, versionCompatibilityPairs)
          .returning(true)

        (reProvisioningStatus.setRunning _)
          .expects()
          .returning(exception.raiseError[IO, Unit])

        (reProvisioningStatus.setRunning _)
          .expects()
          .returning(IO.unit)

        (renkuVersionPairUpdater.update _)
          .expects(versionCompatibilityPairs.head)
          .returning(IO.unit)

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(IO.unit)

        (eventsReScheduler.triggerEventsReScheduling _)
          .expects()
          .returning(IO.unit)

        (reProvisioningStatus.clear _)
          .expects()
          .returning(IO.unit)
      }

      reProvisioning.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Info("The triples are not up to date - re-provisioning is clearing DB"),
        Error("Re-provisioning failure", exception),
        Info(s"Clearing DB finished in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but simply retry if updating CLI version fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {

        (renkuVersionPairFinder.find _)
          .expects()
          .returning(maybeCurrentVersionCompatibilityPair.pure[IO])

        (reprovisionJudge.isReProvisioningNeeded _)
          .expects(maybeCurrentVersionCompatibilityPair, versionCompatibilityPairs)
          .returning(true)

        (reProvisioningStatus.setRunning _)
          .expects()
          .returning(IO.unit)

        (renkuVersionPairUpdater.update _)
          .expects(versionCompatibilityPairs.head)
          .returning(exception.raiseError[IO, Unit])

        (renkuVersionPairUpdater.update _)
          .expects(versionCompatibilityPairs.head)
          .returning(IO.unit)

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(IO.unit)

        (eventsReScheduler.triggerEventsReScheduling _)
          .expects()
          .returning(IO.unit)

        (reProvisioningStatus.clear _)
          .expects()
          .returning(IO.unit)
      }

      reProvisioning.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Info("The triples are not up to date - re-provisioning is clearing DB"),
        Error("Re-provisioning failure", exception),
        Info(s"Clearing DB finished in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but retry from the removing outdated triples step if it fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {

        (renkuVersionPairFinder.find _)
          .expects()
          .returning(maybeCurrentVersionCompatibilityPair.pure[IO])

        (reprovisionJudge.isReProvisioningNeeded _)
          .expects(maybeCurrentVersionCompatibilityPair, versionCompatibilityPairs)
          .returning(true)

        (reProvisioningStatus.setRunning _)
          .expects()
          .returning(IO.unit)

        (renkuVersionPairUpdater.update _)
          .expects(versionCompatibilityPairs.head)
          .returning(IO.unit)

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(exception.raiseError[IO, Unit])

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(IO.unit)

        (eventsReScheduler.triggerEventsReScheduling _)
          .expects()
          .returning(IO.unit)

        (reProvisioningStatus.clear _)
          .expects()
          .returning(IO.unit)
      }

      reProvisioning.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Info("The triples are not up to date - re-provisioning is clearing DB"),
        Error("Re-provisioning failure", exception),
        Info(s"Clearing DB finished in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but retry from the re-scheduling events step if it fails" in new TestCase {
      val exception1 = exceptions.generateOne
      val exception2 = exceptions.generateOne

      inSequence {

        (renkuVersionPairFinder.find _)
          .expects()
          .returning(maybeCurrentVersionCompatibilityPair.pure[IO])

        (reprovisionJudge.isReProvisioningNeeded _)
          .expects(maybeCurrentVersionCompatibilityPair, versionCompatibilityPairs)
          .returning(true)

        (reProvisioningStatus.setRunning _)
          .expects()
          .returning(IO.unit)

        (renkuVersionPairUpdater.update _)
          .expects(versionCompatibilityPairs.head)
          .returning(IO.unit)

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(IO.unit)

        (eventsReScheduler.triggerEventsReScheduling _)
          .expects()
          .returning(exception1.raiseError[IO, Unit])

        (eventsReScheduler.triggerEventsReScheduling _)
          .expects()
          .returning(exception2.raiseError[IO, Unit])

        (eventsReScheduler.triggerEventsReScheduling _)
          .expects()
          .returning(IO.unit)

        (reProvisioningStatus.clear _)
          .expects()
          .returning(IO.unit)
      }

      reProvisioning.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Info("The triples are not up to date - re-provisioning is clearing DB"),
        Error("Re-provisioning failure", exception1),
        Error("Re-provisioning failure", exception2),
        Info(s"Clearing DB finished in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but retry from the clearing the re-provisioning flag step if it fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {

        (renkuVersionPairFinder.find _)
          .expects()
          .returning(maybeCurrentVersionCompatibilityPair.pure[IO])

        (reprovisionJudge.isReProvisioningNeeded _)
          .expects(maybeCurrentVersionCompatibilityPair, versionCompatibilityPairs)
          .returning(true)

        (reProvisioningStatus.setRunning _)
          .expects()
          .returning(IO.unit)

        (renkuVersionPairUpdater.update _)
          .expects(versionCompatibilityPairs.head)
          .returning(IO.unit)

        (triplesRemover.removeAllTriples _)
          .expects()
          .returning(IO.unit)

        (eventsReScheduler.triggerEventsReScheduling _)
          .expects()
          .returning(IO.unit)

        (reProvisioningStatus.clear _)
          .expects()
          .returning(exception.raiseError[IO, Unit])

        (reProvisioningStatus.clear _)
          .expects()
          .returning(IO.unit)
      }

      reProvisioning.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Info("The triples are not up to date - re-provisioning is clearing DB"),
        Error("Re-provisioning failure", exception),
        Info(s"Clearing DB finished in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(global)

  private trait TestCase {
    val versionCompatibilityPairs            = renkuVersionPairs.generateNonEmptyList(2)
    val maybeCurrentVersionCompatibilityPair = renkuVersionPairs.generateOption
    val renkuVersionPairFinder               = mock[RenkuVersionPairFinder[IO]]
    val reprovisionJudge                     = mock[ReProvisionJudge]
    val triplesRemover                       = mock[TriplesRemover[IO]]
    val eventsReScheduler                    = mock[EventsReScheduler[IO]]
    val renkuVersionPairUpdater              = mock[RenkuVersionPairUpdater[IO]]
    val reProvisioningStatus                 = mock[ReProvisioningStatus[IO]]
    val logger                               = TestLogger[IO]()
    val executionTimeRecorder                = TestExecutionTimeRecorder(logger)
    val reProvisioning = new ReProvisioningImpl[IO](
      renkuVersionPairFinder,
      versionCompatibilityPairs,
      reprovisionJudge,
      triplesRemover,
      eventsReScheduler,
      renkuVersionPairUpdater,
      reProvisioningStatus,
      executionTimeRecorder,
      logger,
      250 millis
    )
  }
}
