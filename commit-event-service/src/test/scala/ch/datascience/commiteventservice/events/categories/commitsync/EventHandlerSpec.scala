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

package ch.datascience.commiteventservice.events.categories.commitsync

import cats.effect.concurrent.Deferred
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.Generators._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult}
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventHandlerSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "handle" should {

    "decode an event from the request, " +
      "schedule commit synchronization " +
      s"and return $Accepted - full commit sync event case" in new TestCase {

        val event = fullCommitSyncEvents.generateOne

        (commitEventSynchronizer.synchronizeEvents _)
          .expects(event)
          .returning(().pure[IO])

        handler.handle(requestContent(event.asJson)).getResult shouldBe Accepted

        logger.loggedOnly(Info(s"${logMessageCommon(event)} -> $Accepted"))
      }

    "decode an event from the request, " +
      "schedule commit synchronization " +
      s"and return $Accepted - minimal commit sync event case" in new TestCase {

        val event = minimalCommitSyncEvents.generateOne

        (commitEventSynchronizer.synchronizeEvents _)
          .expects(event)
          .returning(().pure[IO])

        handler.handle(requestContent(event.asJson)).getResult shouldBe Accepted

        logger.loggedOnly(Info(s"${logMessageCommon(event)} -> $Accepted"))
      }

    s"return $Accepted and log an error if scheduling event synchronization fails" in new TestCase {

      val event = commitSyncEvents.generateOne

      (commitEventSynchronizer.synchronizeEvents _)
        .expects(event)
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      handler.handle(requestContent(event.asJson)).getResult shouldBe Accepted

      logger.getMessages(Info).map(_.message) should contain only s"${logMessageCommon(event)} -> $Accepted"

      eventually {
        logger.getMessages(Error).map(_.message) should contain only s"${logMessageCommon(event)} -> Failure"
      }
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": ${categoryName.value}
        }"""
      }

      handler.handle(request).getResult shouldBe BadRequest

      logger.expectNoLogs()
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val commitEventSynchronizer = mock[CommitEventSynchronizer[IO]]
    val logger                  = TestLogger[IO]()
    val handler =
      new EventHandler[IO](categoryName, commitEventSynchronizer, logger)

    def requestContent(event: Json): EventRequestContent = EventRequestContent(event, None)
  }

  private implicit def eventEncoder[E <: CommitSyncEvent]: Encoder[E] = Encoder.instance[E] {
    case FullCommitSyncEvent(id, project, lastSynced) => json"""{
        "categoryName": "COMMIT_SYNC",
        "id":           ${id.value},
        "project": {
          "id":         ${project.id.value},
          "path":       ${project.path.value}
        },
        "lastSynced":   ${lastSynced.value}
      }"""
    case MinimalCommitSyncEvent(project)              => json"""{
        "categoryName": "COMMIT_SYNC",
        "project": {
          "id":         ${project.id.value},
          "path":       ${project.path.value}
        }
      }"""
  }
  private implicit class HandlerOps(handlerResult: IO[(Deferred[IO, Unit], IO[EventSchedulingResult])]) {
    lazy val getResult = handlerResult.unsafeRunSync()._2.unsafeRunSync()
  }
}
