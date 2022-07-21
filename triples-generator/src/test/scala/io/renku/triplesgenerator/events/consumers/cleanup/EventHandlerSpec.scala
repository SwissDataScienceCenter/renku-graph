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

package io.renku.triplesgenerator.events.consumers.cleanup

import cats.data.EitherT
import cats.effect.IO
import cats.implicits.showInterpolator
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ConsumersModelGenerators.notHappySchedulingResults
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{EventHandler => _, _}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with MockFactory with IOSpec with should.Matchers {

  "handle" should {

    "decode an event from the request, clean up " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        givenTsReady

        (eventProcessor.process _)
          .expects(project)
          .returning(IO.unit)

        val eventJson = project.asJson(eventEncoder)
        (eventBodyDeserializer.toCleanUpEvent _).expects(eventJson).returns(CleanUpEvent(project).pure[IO])

        val handlingProcess = handler.createHandlingProcess(requestContent(eventJson)).unsafeRunSync()

        handlingProcess.process.value.unsafeRunSync() shouldBe Right(Accepted)

        handlingProcess.waitToFinish().unsafeRunSync() shouldBe ()

        logger.loggedOnly(
          Info(show"CLEAN_UP: projectId = ${project.id}, projectPath = ${project.path} -> Accepted")
        )
      }

    s"return $BadRequest if project path is malformed" in new TestCase {

      givenTsReady

      val eventJson: Json = json"""{
        "categoryName": "CLEAN_UP",
        "project": {
          "path" :      ${projectPaths.generateOne.value}
        }
      }"""
      val request   = requestContent(eventJson)
      val exception = exceptions.generateOne
      (eventBodyDeserializer.toCleanUpEvent _).expects(eventJson).returns(exception.raiseError[IO, CleanUpEvent])

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }

    s"return $Accepted and release the processing flag when event processor fails while processing the event" in new TestCase {

      givenTsReady

      val exception = exceptions.generateOne
      (eventProcessor.process _)
        .expects(project)
        .returning(exception.raiseError[IO, Unit])

      val eventJson = project.asJson(eventEncoder)
      (eventBodyDeserializer.toCleanUpEvent _).expects(eventJson).returns(CleanUpEvent(project).pure[IO])

      val handlingProcess = handler.createHandlingProcess(requestContent(eventJson)).unsafeRunSync()

      handlingProcess.process.value.unsafeRunSync() shouldBe Right(Accepted)

      handlingProcess.waitToFinish().unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Info(show"CLEAN_UP: projectId = ${project.id}, projectPath = ${project.path} -> Accepted"),
        Error(show"$categoryName: $project failed", exception)
      )
    }

    "return failure if returned from the TS readiness check" in new TestCase {

      val readinessState = notHappySchedulingResults.generateLeft[Accepted]
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(readinessState.pure[IO]))

      val request = requestContent(project.asJson(eventEncoder))

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe readinessState
    }
  }

  private trait TestCase {
    val project = Project(projectIds.generateOne, projectPaths.generateOne)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tsReadinessChecker         = mock[TSReadinessForEventsChecker[IO]]
    val eventProcessor             = mock[EventProcessor[IO]]
    val eventBodyDeserializer      = mock[EventBodyDeserializer[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val handler = new EventHandler[IO](categoryName,
                                       tsReadinessChecker,
                                       eventProcessor,
                                       eventBodyDeserializer,
                                       subscriptionMechanism,
                                       concurrentProcessesLimiter
    )

    (subscriptionMechanism.renewSubscription _).expects().returns(().pure[IO])

    def requestContent(event: Json): EventRequestContent = EventRequestContent.NoPayload(event)

    def givenTsReady =
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(Accepted.asRight[EventSchedulingResult].pure[IO]))
  }

  implicit lazy val eventEncoder: Encoder[Project] =
    Encoder.instance[Project] { project =>
      json"""{
        "categoryName": "CLEAN_UP",
        "project": {
          "id":    ${project.id.value},
          "path" : ${project.path.value}
        }
      }"""
    }

  private implicit class EventHandlingProcessOps(handlingProcess: IO[EventHandlingProcess[IO]]) {
    def unsafeRunSyncProcess() =
      handlingProcess.unsafeRunSync().process.value.unsafeRunSync()
  }
}
