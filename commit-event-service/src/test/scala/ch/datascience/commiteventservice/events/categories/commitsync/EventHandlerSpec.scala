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

import Generators.commitSyncEvents
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.events.consumers.EventRequestContent
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
      s"and return $Accepted" in new TestCase {

        val event = commitSyncEvents.generateOne

        (missedEventsGenerator.generateMissedEvents _)
          .expects(event)
          .returning(().pure[IO])

        handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: id = ${event.id}, projectId = ${event.project.id}, projectPath = ${event.project.path}, lastSynced = ${event.lastSynced} -> $Accepted"
          )
        )
      }

    s"return $Accepted and log an error if scheduling missed events generation fails" in new TestCase {

      val event = commitSyncEvents.generateOne

      (missedEventsGenerator.generateMissedEvents _)
        .expects(event)
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

      logger.getMessages(Info).map(_.message) should contain(
        s"${handler.categoryName}: id = ${event.id}, projectId = ${event.project.id}, projectPath = ${event.project.path}, lastSynced = ${event.lastSynced} -> $Accepted"
      )

      eventually {
        logger.getMessages(Error).map(_.message) should contain(
          s"${handler.categoryName}: id = ${event.id}, projectId = ${event.project.id}, projectPath = ${event.project.path}, lastSynced = ${event.lastSynced} -> Failure"
        )
      }
    }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      handler.handle(requestContent(jsons.generateOne.asJson)).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": ${categoryName.value}
        }"""
      }

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {

    val missedEventsGenerator = mock[MissedEventsGenerator[IO]]
    val logger                = TestLogger[IO]()
    val handler               = new EventHandler[IO](categoryName, missedEventsGenerator, logger)

    def requestContent(event: Json): EventRequestContent = EventRequestContent(event, None)
  }

  private implicit lazy val eventEncoder: Encoder[CommitSyncEvent] = Encoder.instance[CommitSyncEvent] { event =>
    json"""{
      "categoryName": "COMMIT_SYNC",
      "id":           ${event.id.value},
      "project": {
        "id":   ${event.project.id.value},
        "path": ${event.project.path.value}
      },
      "lastSynced": ${event.lastSynced.value}
    }"""
  }
}
