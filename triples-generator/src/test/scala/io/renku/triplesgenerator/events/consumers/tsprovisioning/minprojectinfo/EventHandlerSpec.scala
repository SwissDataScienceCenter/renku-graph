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
package tsprovisioning.minprojectinfo

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
import io.renku.events.consumers.EventSchedulingResult.Accepted
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, EventSchedulingResult}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event from the request, " +
      "schedule triples transformation " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        givenTsReady

        val event = minProjectInfoEvents.generateOne

        (eventProcessor.process _)
          .expects(event)
          .returning(().pure[IO])

        val request = requestContent(event.asJson)

        handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Right(Accepted)

        logger.loggedOnly(Info(show"$categoryName: $event -> $Accepted"))
      }

    s"return $Accepted when event processor fails processing the event" in new TestCase {

      givenTsReady

      val event = minProjectInfoEvents.generateOne

      (eventProcessor.process _)
        .expects(event)
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      val request = requestContent(event.asJson)

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Right(Accepted)

      logger.loggedOnly(Info(show"$categoryName: $event -> $Accepted"))
    }

    "return failure if returned from the TS readiness check" in new TestCase {

      val readinessState = notHappySchedulingResults.generateLeft[Accepted]
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(readinessState.pure[IO]))

      val request = requestContent(minProjectInfoEvents.generateOne.asJson)

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe readinessState
    }
  }

  private trait TestCase {

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val tsReadinessChecker         = mock[TSReadinessForEventsChecker[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val eventProcessor             = mock[EventProcessor[IO]]
    val handler = new EventHandler[IO](categoryName,
                                       concurrentProcessesLimiter,
                                       tsReadinessChecker,
                                       subscriptionMechanism,
                                       eventProcessor
    )

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)

    def requestContent(event: Json): EventRequestContent = events.EventRequestContent.NoPayload(event)

    def givenTsReady =
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(Accepted.asRight[EventSchedulingResult].pure[IO]))
  }

  private implicit lazy val eventEncoder: Encoder[MinProjectInfoEvent] =
    Encoder.instance[MinProjectInfoEvent] { case MinProjectInfoEvent(project) =>
      json"""{
        "categoryName": "ADD_MIN_PROJECT_INFO",
        "project": {
          "id" :  ${project.id.value},
          "path": ${project.path.value}
        }
      }"""
    }

  private implicit class EventHandlingProcessOps(handlingProcess: IO[EventHandlingProcess[IO]]) {
    def unsafeRunSyncProcess() = handlingProcess.unsafeRunSync().process.value.unsafeRunSync()
  }
}
