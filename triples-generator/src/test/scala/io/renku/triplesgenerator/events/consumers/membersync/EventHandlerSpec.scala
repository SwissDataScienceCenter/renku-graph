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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ConsumersModelGenerators.notHappySchedulingResults
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.{EventHandlingProcess, EventSchedulingResult}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event from the request, " +
      "schedule members sync " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        givenTsReady

        (membersSynchronizer.synchronizeMembers _)
          .expects(projectPath)
          .returning(IO.unit)

        val request = requestContent(projectPath.asJson(eventEncoder))

        handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Right(Accepted)

        logger.loggedOnly(Info(s"${handler.categoryName}: projectPath = $projectPath -> $Accepted"))
      }

    s"return $BadRequest if project path is malformed" in new TestCase {

      givenTsReady

      val request = requestContent(json"""{
        "categoryName": "MEMBER_SYNC",
        "project": {
          "path" :      ${nonNegativeInts().generateOne.value}
        }
      }""")

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }

    "return failure if returned from the TS readiness check" in new TestCase {

      val readinessState = notHappySchedulingResults.generateLeft[Accepted]
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(readinessState.pure[IO]))

      val request = requestContent(projectPath.asJson(eventEncoder))

      handler.createHandlingProcess(request).unsafeRunSyncProcess() shouldBe readinessState
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tsReadinessChecker  = mock[TSReadinessForEventsChecker[IO]]
    val membersSynchronizer = mock[MembersSynchronizer[IO]]
    val handler             = new EventHandler[IO](categoryName, tsReadinessChecker, membersSynchronizer)

    def requestContent(event: Json): EventRequestContent = EventRequestContent.NoPayload(event)

    def givenTsReady =
      (() => tsReadinessChecker.verifyTSReady)
        .expects()
        .returning(EitherT(Accepted.asRight[EventSchedulingResult].pure[IO]))
  }

  implicit lazy val eventEncoder: Encoder[projects.Path] =
    Encoder.instance[projects.Path] { projectPath =>
      json"""{
        "categoryName": "MEMBER_SYNC",
        "project": {
          "path" :      ${projectPath.value}
        }
      }"""
    }

  private implicit class EventHandlingProcessOps(handlingProcess: IO[EventHandlingProcess[IO]]) {
    def unsafeRunSyncProcess() =
      handlingProcess.unsafeRunSync().process.value.unsafeRunSync()
  }
}
