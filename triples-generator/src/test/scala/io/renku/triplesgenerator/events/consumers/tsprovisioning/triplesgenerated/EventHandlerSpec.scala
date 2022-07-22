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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesgenerated

import CategoryGenerators._
import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.events
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ConsumersModelGenerators.notHappySchedulingResults
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{EventHandler => _, _}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.zippedEventPayloads
import io.renku.graph.model.events.{CompoundEventId, ZippedEventPayload}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event from the request, " +
      "schedule triples transformation " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        givenTsReady

        (eventBodyDeserializer.toEvent _)
          .expects(event.compoundEventId, event.project, zippedPayload)
          .returning(event.pure[IO])

        (eventProcessor.process _)
          .expects(event)
          .returning(().pure[IO])

        val request = requestContent((event.compoundEventId, event.project).asJson, zippedPayload)

        val handlingProcess = handler.createHandlingProcess(request).unsafeRunSync()

        handlingProcess.process.value.unsafeRunSync()  shouldBe Right(Accepted)
        handlingProcess.waitToFinish().unsafeRunSync() shouldBe ()

        logger.loggedOnly(Info(show"$categoryName: $event -> $Accepted"))
      }

    s"return $BadRequest if event payload is not present" in new TestCase {

      givenTsReady

      val request = EventRequestContent.NoPayload((event.compoundEventId, event.project).asJson)

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }

    s"return $BadRequest if event payload is malformed" in new TestCase {

      givenTsReady

      val request =
        EventRequestContent.WithPayload((event.compoundEventId, event.project).asJson, jsons.generateOne.noSpaces)

      handler.createHandlingProcess(request).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(
        BadRequest
      )

      logger.expectNoLogs()
    }

    s"return $Accepted and release the processing flag when event processor fails processing the event" in new TestCase {

      givenTsReady

      (eventBodyDeserializer.toEvent _)
        .expects(event.compoundEventId, event.project, zippedPayload)
        .returning(event.pure[IO])

      val exception = exceptions.generateOne
      (eventProcessor.process _)
        .expects(event)
        .returning(exception.raiseError[IO, Unit])

      val request = requestContent((event.compoundEventId, event.project).asJson, zippedPayload)

      val handlingProcess = handler.createHandlingProcess(request).unsafeRunSync()

      handlingProcess.process.value.unsafeRunSync()  shouldBe Right(Accepted)
      handlingProcess.waitToFinish().unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Info(show"$categoryName: $event -> $Accepted"),
        Error(show"$categoryName: $event failed", exception)
      )
    }

    "return failure if returned from the TS readiness check" in new TestCase {

      val readinessState = notHappySchedulingResults.generateLeft[Accepted]
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(readinessState.pure[IO]))

      val request = requestContent((event.compoundEventId, event.project).asJson, zippedPayload)

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe readinessState
    }
  }

  private trait TestCase {

    val event         = triplesGeneratedEvents.generateOne
    val zippedPayload = zippedEventPayloads.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tsReadinessChecker         = mock[TSReadinessForEventsChecker[IO]]
    val eventProcessor             = mock[EventProcessor[IO]]
    val eventBodyDeserializer      = mock[EventBodyDeserializer[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val handler = new EventHandler[IO](categoryName,
                                       tsReadinessChecker,
                                       eventBodyDeserializer,
                                       subscriptionMechanism,
                                       concurrentProcessesLimiter,
                                       eventProcessor
    )

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)

    def requestContent(event: Json, payload: ZippedEventPayload): EventRequestContent =
      events.EventRequestContent.WithPayload(event, payload)

    def givenTsReady =
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(Accepted.asRight[EventSchedulingResult].pure[IO]))
  }

  private implicit lazy val eventEncoder: Encoder[(CompoundEventId, Project)] =
    Encoder.instance[(CompoundEventId, Project)] { case (eventId, project) =>
      json"""{
        "categoryName": "TRIPLES_GENERATED",
        "id":           ${eventId.id.value},
        "project": {
          "id" :  ${eventId.projectId.value},
          "path": ${project.path.value}
        }
      }"""
    }

  private implicit class EventHandlingProcessOps(handlingProcess: IO[EventHandlingProcess[IO]]) {
    def unsafeRunSyncProcess() = handlingProcess.unsafeRunSync().process.value.unsafeRunSync()
  }
}
