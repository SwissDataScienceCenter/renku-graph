/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.reprovisioning

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.producers.EventSender.EventContext
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.ConditionedMigration.MigrationRequired
import io.renku.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class ReProvisioningSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "required" should {

    "return No if TS is up to date" in new TestCase {

      (reprovisionJudge.reProvisioningNeeded _).expects().returning(false.pure[IO])

      reProvisioning.required.unsafeRunSync() shouldBe MigrationRequired.No("TS up to date")
    }

    "return Yes if TS is not up to date" in new TestCase {

      (reprovisionJudge.reProvisioningNeeded _).expects().returning(true.pure[IO])

      reProvisioning.required.unsafeRunSync() shouldBe MigrationRequired.Yes("TS in incompatible version")
    }

    "retry checking if re-provisioning is needed on failure" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {
        (reprovisionJudge.reProvisioningNeeded _).expects().returning(exception.raiseError[IO, Boolean])
        (reprovisionJudge.reProvisioningNeeded _).expects().returning(false.pure[IO])
      }

      reProvisioning.required.unsafeRunSync() shouldBe MigrationRequired.No("TS up to date")
    }
  }

  "migrate" should {

    "clear the RDF Store and reschedule all Commit Events for processing in the Log if store is outdated" in new TestCase {

      inSequence {
        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[IO])

        (reProvisioningStatus.setRunning _).expects(controller).returning(IO.unit)

        (renkuVersionPairUpdater.update _).expects(configuredRenkuVersionPairs.head).returning(IO.unit)

        (triplesRemover.removeAllTriples _).expects().returning(IO.unit)

        expectStatusChangeEventSucceeds()

        (reProvisioningStatus.clear _).expects().returning(IO.unit)
      }

      reProvisioning.migrate().value.unsafeRunSync() shouldBe ().asRight

      logger.loggedOnly(
        Info(s"re-provisioning: TS cleared in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but simply retry if finding microservice URL fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {

        (microserviceUrlFinder.findBaseUrl _).expects().returning(exception.raiseError[IO, MicroserviceBaseUrl])
        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[IO])

        (reProvisioningStatus.setRunning _).expects(controller).returning(IO.unit)

        (renkuVersionPairUpdater.update _).expects(configuredRenkuVersionPairs.head).returning(IO.unit)

        (triplesRemover.removeAllTriples _).expects().returning(IO.unit)

        expectStatusChangeEventSucceeds()

        (reProvisioningStatus.clear _).expects().returning(IO.unit)
      }

      reProvisioning.migrate().value.unsafeRunSync() shouldBe ().asRight

      logger.loggedOnly(
        Error("re-provisioning: failure", exception),
        Info(s"re-provisioning: TS cleared in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but simply retry if setRunning method fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {

        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[IO])

        (reProvisioningStatus.setRunning _).expects(controller).returning(exception.raiseError[IO, Unit])
        (reProvisioningStatus.setRunning _).expects(controller).returning(IO.unit)

        (renkuVersionPairUpdater.update _).expects(configuredRenkuVersionPairs.head).returning(IO.unit)

        (triplesRemover.removeAllTriples _).expects().returning(IO.unit)

        expectStatusChangeEventSucceeds()

        (reProvisioningStatus.clear _).expects().returning(IO.unit)
      }

      reProvisioning.migrate().value.unsafeRunSync() shouldBe ().asRight

      logger.loggedOnly(
        Error("re-provisioning: failure", exception),
        Info(s"re-provisioning: TS cleared in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but simply retry if updating CLI version fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {

        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[IO])

        (reProvisioningStatus.setRunning _).expects(controller).returning(IO.unit)

        (renkuVersionPairUpdater.update _)
          .expects(configuredRenkuVersionPairs.head)
          .returning(exception.raiseError[IO, Unit])
        (renkuVersionPairUpdater.update _).expects(configuredRenkuVersionPairs.head).returning(IO.unit)

        (triplesRemover.removeAllTriples _).expects().returning(IO.unit)

        expectStatusChangeEventSucceeds()

        (reProvisioningStatus.clear _).expects().returning(IO.unit)
      }

      reProvisioning.migrate().value.unsafeRunSync() shouldBe ().asRight

      logger.loggedOnly(
        Error("re-provisioning: failure", exception),
        Info(s"re-provisioning: TS cleared in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but retry from the removing outdated triples step if it fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {

        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[IO])

        (reProvisioningStatus.setRunning _).expects(controller).returning(IO.unit)

        (renkuVersionPairUpdater.update _).expects(configuredRenkuVersionPairs.head).returning(IO.unit)

        (triplesRemover.removeAllTriples _).expects().returning(exception.raiseError[IO, Unit])

        (triplesRemover.removeAllTriples _).expects().returning(IO.unit)

        expectStatusChangeEventSucceeds()

        (reProvisioningStatus.clear _).expects().returning(IO.unit)
      }

      reProvisioning.migrate().value.unsafeRunSync() shouldBe ().asRight

      logger.loggedOnly(
        Error("re-provisioning: failure", exception),
        Info(s"re-provisioning: TS cleared in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but retry from the re-scheduling events step if it fails" in new TestCase {
      val exception1 = exceptions.generateOne
      val exception2 = exceptions.generateOne

      inSequence {

        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[IO])

        (reProvisioningStatus.setRunning _).expects(controller).returning(IO.unit)

        (renkuVersionPairUpdater.update _).expects(configuredRenkuVersionPairs.head).returning(IO.unit)

        (triplesRemover.removeAllTriples _).expects().returning(IO.unit)

        expectStatusChangeEventFailing(exception1)
        expectStatusChangeEventFailing(exception2)
        expectStatusChangeEventSucceeds()

        (reProvisioningStatus.clear _).expects().returning(IO.unit)
      }

      reProvisioning.migrate().value.unsafeRunSync() shouldBe ().asRight

      logger.loggedOnly(
        Error("re-provisioning: failure", exception1),
        Error("re-provisioning: failure", exception2),
        Info(s"re-provisioning: TS cleared in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }

    "do not fail but retry from the clearing the re-provisioning flag step if it fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {

        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[IO])

        (reProvisioningStatus.setRunning _).expects(controller).returning(IO.unit)

        (renkuVersionPairUpdater.update _).expects(configuredRenkuVersionPairs.head).returning(IO.unit)

        (triplesRemover.removeAllTriples _).expects().returning(IO.unit)

        expectStatusChangeEventSucceeds()

        (reProvisioningStatus.clear _).expects().returning(exception.raiseError[IO, Unit])
        (reProvisioningStatus.clear _).expects().returning(IO.unit)
      }

      reProvisioning.migrate().value.unsafeRunSync() shouldBe ().asRight

      logger.loggedOnly(
        Error("re-provisioning: failure", exception),
        Info(s"re-provisioning: TS cleared in ${executionTimeRecorder.elapsedTime}ms - re-processing all the events")
      )
    }
  }

  private trait TestCase {
    val controller = microserviceBaseUrls.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val configuredRenkuVersionPairs = renkuVersionPairs.generateNonEmptyList(2)
    val reprovisionJudge            = mock[ReProvisionJudge[IO]]
    val triplesRemover              = mock[TriplesRemover[IO]]
    val eventSender                 = mock[EventSender[IO]]
    val renkuVersionPairUpdater     = mock[RenkuVersionPairUpdater[IO]]
    val microserviceUrlFinder       = mock[MicroserviceUrlFinder[IO]]
    val reProvisioningStatus        = mock[ReProvisioningStatus[IO]]
    val executionTimeRecorder       = TestExecutionTimeRecorder[IO]()
    val reProvisioning = new ReProvisioningImpl[IO](
      configuredRenkuVersionPairs,
      reprovisionJudge,
      triplesRemover,
      eventSender,
      renkuVersionPairUpdater,
      microserviceUrlFinder,
      reProvisioningStatus,
      executionTimeRecorder,
      250 millis
    )

    def expectStatusChangeEventSucceeds() = expectStatusChangeReturning(().pure[IO])

    def expectStatusChangeEventFailing(exception: Exception) =
      expectStatusChangeReturning(exception.raiseError[IO, Unit])

    def expectStatusChangeReturning(result: IO[Unit]) =
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventContext))
        .expects(
          EventRequestContent.NoPayload(json"""{"categoryName": "EVENTS_STATUS_CHANGE", "newStatus": "NEW"}"""),
          EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                                   "re-provisioning: sending EVENTS_STATUS_CHANGE failed"
          )
        )
        .returning(result)
  }
}
