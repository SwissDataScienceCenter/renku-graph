/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.circe.literal._
import io.renku.data.Message
import io.renku.events.Generators.{eventRequestContents, subscriberUrls}
import io.renku.events.consumers.EventSchedulingResult.{SchedulingError, ServiceUnavailable}
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
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
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import org.scalacheck.Gen
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers with EitherValues {

  "handlingDefinition.decode" should {

    "be the eventDecoder.decode" in new TestCase {

      givenTsStateGreen

      val request = eventRequestContents.generateOne

      decodingFunction.expects(request).returning(().asRight)

      handler.createHandlingDefinition().decode(request).value shouldBe ()
    }
  }

  "handlingDefinition.process" should {

    "kick off migrations and send the migration status change with 'DONE' on success" in new TestCase {

      givenTsStateGreen

      (migrationsRunner.run _).expects().returns(rightT(()))

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventContext))
        .expects(statusChangePayload(status = "DONE"), expectedEventContext)
        .returning(().pure[IO])

      handler.createHandlingDefinition().process(()).unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info(show"$categoryName: $serviceVersion accepted"))
    }

    "kick off migrations and send the migration status change with 'RECOVERABLE_FAILURE' on recoverable failure" in new TestCase {

      givenTsStateGreen

      val recoverableFailure = processingRecoverableErrors.generateOne
      (migrationsRunner.run _).expects().returning(leftT(recoverableFailure))

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventContext))
        .expects(
          statusChangePayload(
            status = "RECOVERABLE_FAILURE",
            Message.Error.fromMessageAndStackTraceUnsafe(recoverableFailure.message, recoverableFailure.cause).show.some
          ),
          expectedEventContext
        )
        .returning(().pure[IO])

      handler.createHandlingDefinition().process(()).unsafeRunSync() shouldBe ()
    }

    "kick off migrations and send the migration status change with 'NON_RECOVERABLE_FAILURE' on non recoverable failure" in new TestCase {

      givenTsStateGreen

      val exception = exceptions.generateOne
      (migrationsRunner.run _).expects().returning(liftF(exception.raiseError[IO, Unit]))

      val payloadCaptor = CaptureOne[EventRequestContent.NoPayload]()
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventContext))
        .expects(capture(payloadCaptor), expectedEventContext)
        .returning(().pure[IO])

      handler.createHandlingDefinition().process(()).unsafeRunSync() shouldBe ()

      val eventCursor = payloadCaptor.value.event.hcursor
      eventCursor.downField("newStatus").as[String] shouldBe "NON_RECOVERABLE_FAILURE".asRight
      eventCursor.downField("message").as[String].fold(throw _, identity) should
        include(Message.Error.fromStackTrace(exception).show)
    }
  }

  "handlingDefinition.precondition" should {

    TSState.Ready :: TSState.MissingDatasets :: TSState.Migrating :: Nil foreach { state =>
      s"return None when TSState is $state" in new TestCase {

        givenTsState(returning = state.pure[IO])

        handler.createHandlingDefinition().precondition.unsafeRunSync() shouldBe None
      }
    }

    s"return ServiceUnavailable when TSState is ${TSState.ReProvisioning}" in new TestCase {

      givenTsState(returning = TSState.ReProvisioning.pure[IO])

      handler.createHandlingDefinition().precondition.unsafeRunSync() shouldBe
        ServiceUnavailable(TSState.ReProvisioning.widen.show).some
    }

    "return SchedulingError if TSState check fails" in new TestCase {

      val exception = exceptions.generateOne
      givenTsState(returning = exception.raiseError[IO, TSState])

      handler.createHandlingDefinition().precondition.unsafeRunSync() shouldBe SchedulingError(exception).some
    }
  }

  "handlingDefinition.onRelease" should {

    "be the SubscriptionMechanism.renewSubscription" in new TestCase {

      givenTsStateGreen

      handler.createHandlingDefinition().onRelease.foreach(_.unsafeRunSync())

      renewSubscriptionCalled.get.unsafeRunSync() shouldBe true
    }
  }

  private trait TestCase {

    private val subscriberUrl = subscriberUrls.generateOne
    private val serviceId     = microserviceIdentifiers.generateOne
    val serviceVersion        = serviceVersions.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    private val tsStateChecker = mock[TSStateChecker[IO]]
    val migrationsRunner       = mock[MigrationsRunner[IO]]
    val eventSender            = mock[EventSender[IO]]

    private val subscriptionMechanism = mock[SubscriptionMechanism[IO]]
    val renewSubscriptionCalled       = Ref.unsafe[IO, Boolean](false)
    (subscriptionMechanism.renewSubscription _).expects().returns(renewSubscriptionCalled.set(true))

    val decodingFunction     = mockFunction[EventRequestContent, Either[Exception, Unit]]
    private val eventDecoder = mock[EventDecoder]
    (eventDecoder.decode _).expects(serviceVersion).returning(decodingFunction)

    val handler = new EventHandler[IO](subscriberUrl,
                                       serviceId,
                                       serviceVersion,
                                       tsStateChecker,
                                       migrationsRunner,
                                       eventSender,
                                       subscriptionMechanism,
                                       mock[ProcessExecutor[IO]],
                                       eventDecoder
    )

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

    def givenTsStateGreen = givenTsState(
      Gen.oneOf(TSState.Ready, TSState.MissingDatasets).generateOne.pure[IO]
    )

    def givenTsState(returning: IO[TSState]) =
      (() => tsStateChecker.checkTSState)
        .expects()
        .returning(returning)
  }

  private lazy val expectedEventContext =
    EventContext(
      CategoryName("MIGRATION_STATUS_CHANGE"),
      show"$categoryName: sending status change event failed"
    )
}
