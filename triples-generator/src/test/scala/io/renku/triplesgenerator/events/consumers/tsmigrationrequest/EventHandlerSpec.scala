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

package io.renku.triplesgenerator.events.consumers
package tsmigrationrequest

import TSStateChecker.TSState
import cats.data.EitherT.{leftT, liftF, rightT}
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.config.ServiceVersion
import io.renku.data.ErrorMessage
import io.renku.events.consumers.ConcurrentProcessesLimiter
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.{SubscriptionMechanism, subscriberUrls}
import io.renku.events.producers.EventSender
import io.renku.events.producers.EventSender.EventContext
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.CommonGraphGenerators.{microserviceIdentifiers, serviceVersions}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.json.JsonOps._
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalacheck.Gen
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "tryHandling" should {

    "return ServiceUnavailable if TSState is ReProvisioning" in new TestCase {
      (() => tsStateChecker.checkTSState).expects().returning(TSState.ReProvisioning.pure[IO])

      val process = handler.createHandlingProcess(requestPayload(serviceVersion)).unsafeRunSync()

      process.process.merge.unsafeRunSync() shouldBe ServiceUnavailable("Re-provisioning running")
    }

    "return SchedulingError if TSState check fails" in new TestCase {
      val exception = exceptions.generateOne
      (() => tsStateChecker.checkTSState).expects().returning(exception.raiseError[IO, TSState])

      val process = handler.createHandlingProcess(requestPayload(serviceVersion)).unsafeRunSync()

      process.process.merge.unsafeRunSync() shouldBe SchedulingError(exception)
    }

    TSState.Ready :: TSState.MissingDatasets :: Nil foreach { tsState =>
      "decode an event from the request, " +
        "check if the requested version matches this TG version " +
        "kick off the Migrations Runner, " +
        "send MIGRATION_STATUS_CHANGE event with status DONE " +
        show"and return $Accepted " +
        show"if TSState is $tsState" in new TestCase {

          (() => tsStateChecker.checkTSState).expects().returning(tsState.pure[IO])

          (migrationsRunner.run _).expects().returning(rightT(()))

          (eventSender
            .sendEvent(_: EventRequestContent.NoPayload, _: EventContext))
            .expects(statusChangePayload(status = "DONE"), expectedEventContext)
            .returning(().pure[IO])

          val process = handler.createHandlingProcess(requestPayload(serviceVersion)).unsafeRunSync()

          process.process.merge.unsafeRunSync() shouldBe Accepted

          process.waitToFinish().unsafeRunSync()

          logger.loggedOnly(Info(show"$categoryName: $serviceVersion -> $Accepted"))
        }
    }

    "send MIGRATION_STATUS_CHANGE event with status RECOVERABLE_FAILURE " +
      "when migration returns ProcessingRecoverableError " +
      s"and return $Accepted" in new TestCase {

        givenTsStateGreen

        val recoverableFailure = processingRecoverableErrors.generateOne
        (migrationsRunner.run _).expects().returning(leftT(recoverableFailure))

        (eventSender
          .sendEvent(_: EventRequestContent.NoPayload, _: EventContext))
          .expects(
            statusChangePayload(
              status = "RECOVERABLE_FAILURE",
              ErrorMessage.withMessageAndStackTrace(recoverableFailure.message, recoverableFailure.cause).value.some
            ),
            expectedEventContext
          )
          .returning(().pure[IO])

        val handlingProcess = handler.createHandlingProcess(requestPayload(serviceVersion)).unsafeRunSync()

        handlingProcess.process.merge.unsafeRunSync() shouldBe Accepted

        handlingProcess.waitToFinish().unsafeRunSync() shouldBe ()

        logger.loggedOnly(Info(show"$categoryName: $serviceVersion -> $Accepted"))
      }

    "send MIGRATION_STATUS_CHANGE event with status NON_RECOVERABLE_FAILURE " +
      "when migration fails " +
      s"and return $Accepted" in new TestCase {

        givenTsStateGreen

        val exception = exceptions.generateOne
        (migrationsRunner.run _).expects().returning(liftF(exception.raiseError[IO, Unit]))

        val payloadCaptor = CaptureOne[EventRequestContent.NoPayload]()
        (eventSender
          .sendEvent(_: EventRequestContent.NoPayload, _: EventContext))
          .expects(capture(payloadCaptor), expectedEventContext)
          .returning(().pure[IO])

        val handlingProcess = handler.createHandlingProcess(requestPayload(serviceVersion)).unsafeRunSync()

        handlingProcess.process.merge.unsafeRunSync() shouldBe Accepted

        handlingProcess.waitToFinish().unsafeRunSync() shouldBe ()

        val actualEvent = payloadCaptor.value.event
        actualEvent.hcursor.downField("newStatus").as[String] shouldBe "NON_RECOVERABLE_FAILURE".asRight
        actualEvent.hcursor.downField("message").as[String].fold(throw _, identity) should
          include(ErrorMessage.withStackTrace(exception).value)

        logger.loggedOnly(Info(show"$categoryName: $serviceVersion -> $Accepted"))
      }

    s"fail with $BadRequest for malformed event" in new TestCase {

      givenTsStateGreen

      handler
        .createHandlingProcess(EventRequestContent.NoPayload(json"""{"categoryName": ${categoryName.value}}"""))
        .flatMap(_.process.merge)
        .unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    "decode an event from the request, " +
      s"and return $BadRequest check if the requested version is different than this TG version" in new TestCase {

        givenTsStateGreen

        val requestedVersion = serviceVersions.generateOne
        handler
          .createHandlingProcess(requestPayload(requestedVersion))
          .flatMap(_.process.merge)
          .unsafeRunSync() shouldBe BadRequest

        logger.loggedOnly(
          Info(show"$categoryName: $requestedVersion -> $BadRequest service in version '$serviceVersion'")
        )
      }
  }

  private trait TestCase {

    val subscriberUrl              = subscriberUrls.generateOne
    val serviceId                  = microserviceIdentifiers.generateOne
    val serviceVersion             = serviceVersions.generateOne
    val tsStateChecker             = mock[TSStateChecker[IO]]
    val migrationsRunner           = mock[MigrationsRunner[IO]]
    val eventSender                = mock[EventSender[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val handler = new EventHandler[IO](subscriberUrl,
                                       serviceId,
                                       serviceVersion,
                                       tsStateChecker,
                                       migrationsRunner,
                                       eventSender,
                                       subscriptionMechanism,
                                       concurrentProcessesLimiter
    )

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)

    def statusChangePayload(status: String, maybeMessage: Option[String] = None) =
      EventRequestContent.NoPayload(json"""{
        "categoryName": "MIGRATION_STATUS_CHANGE",
        "subscriber": {
          "url":     ${subscriberUrl.value},
          "id":      ${serviceId.value},
          "version": ${serviceVersion.value}
        },
        "newStatus": $status
      }
      """ addIfDefined ("message" -> maybeMessage))

    def requestPayload(version: ServiceVersion) = EventRequestContent.NoPayload(json"""{
      "categoryName": "TS_MIGRATION_REQUEST",
      "subscriber": {
        "version": ${version.value}
      }
    }
    """)

    def givenTsStateGreen =
      (() => tsStateChecker.checkTSState)
        .expects()
        .returning(Gen.oneOf(TSState.Ready, TSState.MissingDatasets).generateOne.pure[IO])
  }

  private lazy val expectedEventContext =
    EventContext(CategoryName("MIGRATION_STATUS_CHANGE"), show"$categoryName: sending status change event failed")
}
