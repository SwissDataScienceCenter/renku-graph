/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ConsumersModelGenerators.notHappySchedulingResults
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, EventSchedulingResult}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import io.renku.graph.model.events.CompoundEventId
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.EventProcessingGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "createHandlingProcess" should {

    "decode an event from the request, " +
      "schedule triples generation " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        givenTsReady

        val commitEvent = commitEvents.generateOne
        (eventBodyDeserializer.toCommitEvent _)
          .expects(eventBody)
          .returning(commitEvent.pure[IO])

        (eventProcessor.process _)
          .expects(commitEvent)
          .returning(().pure[IO])

        val request = requestContent(commitEvent.compoundEventId.asJson(eventEncoder), eventBody.value)

        val handlingProcess = handler.createHandlingProcess(request).unsafeRunSync()

        handlingProcess.process.value.unsafeRunSync() shouldBe Right(Accepted)

        handlingProcess.waitToFinish().unsafeRunSync() shouldBe ()

        logger.loggedOnly(
          Info(
            show"$categoryName: ${commitEvent.compoundEventId}, projectPath = ${commitEvent.project.path} -> $Accepted"
          )
        )
      }

    s"return $BadRequest if event body is not correct" in new TestCase {

      givenTsReady

      val request = requestContent(compoundEventIds.generateOne.asJson(eventEncoder), eventBody.value)

      (eventBodyDeserializer.toCommitEvent _)
        .expects(eventBody)
        .returning(exceptions.generateOne.raiseError[IO, CommitEvent])

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Left(BadRequest)
    }

    s"return $BadRequest if event body is not present" in new TestCase {

      givenTsReady

      val request = EventRequestContent.NoPayload(compoundEventIds.generateOne.asJson(eventEncoder))

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Left(BadRequest)
    }

    s"return $Accepted and release the processing flag when event processor fails while processing the event" in new TestCase {

      givenTsReady

      val commitEvent = commitEvents.generateOne
      (eventBodyDeserializer.toCommitEvent _)
        .expects(eventBody)
        .returning(commitEvent.pure[IO])

      val exception = exceptions.generateOne
      (eventProcessor.process _)
        .expects(commitEvent)
        .returning(exception.raiseError[IO, Unit])

      val request = requestContent(commitEvent.compoundEventId.asJson(eventEncoder), eventBody.value)

      val handlingProcess = handler.createHandlingProcess(request).unsafeRunSync()

      handlingProcess.process.value.unsafeRunSync() shouldBe Accepted.asRight

      handlingProcess.waitToFinish().unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Info(
          show"$categoryName: ${commitEvent.compoundEventId}, projectPath = ${commitEvent.project.path} -> $Accepted"
        ),
        Error(show"$categoryName: $commitEvent failed", exception)
      )
    }

    "return failure if returned from the TS readiness check" in new TestCase {

      val readinessState = notHappySchedulingResults.generateLeft[Accepted]
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(readinessState.pure[IO]))

      val commitEvent = commitEvents.generateOne
      val request     = requestContent(commitEvent.compoundEventId.asJson(eventEncoder), eventBody.value)

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe readinessState
    }
  }

  private trait TestCase {

    val eventBody = eventBodies.generateOne

    val tsReadinessChecker         = mock[TSReadinessForEventsChecker[IO]]
    val eventProcessor             = mock[EventProcessor[IO]]
    val eventBodyDeserializer      = mock[EventBodyDeserializer[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)

    val handler = new EventHandler[IO](categoryName,
                                       tsReadinessChecker,
                                       eventProcessor,
                                       eventBodyDeserializer,
                                       subscriptionMechanism,
                                       concurrentProcessesLimiter
    )

    def requestContent(event: Json, payload: String): EventRequestContent.WithPayload[String] =
      EventRequestContent.WithPayload(event, payload)

    def givenTsReady =
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(Accepted.asRight[EventSchedulingResult].pure[IO]))
  }

  private implicit lazy val eventEncoder: Encoder[CompoundEventId] =
    Encoder.instance[CompoundEventId] { eventId =>
      json"""{
        "categoryName": "AWAITING_GENERATION",
        "id":           ${eventId.id.value},
        "project": {
          "id" :        ${eventId.projectId.value}
        }
      }"""
    }

  private implicit class EventHandlingProcessOps(handlingProcessIO: IO[EventHandlingProcess[IO]]) {
    def unsafeRunSyncProcess() =
      handlingProcessIO.unsafeRunSync().process.value.unsafeRunSync()
  }
}
