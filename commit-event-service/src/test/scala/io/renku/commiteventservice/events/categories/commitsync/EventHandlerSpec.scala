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

package io.renku.commiteventservice.events.categories.commitsync

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.commiteventservice.events.categories.commitsync.Generators._
import io.renku.commiteventservice.events.categories.commitsync.eventgeneration.CommitsSynchronizer
import io.renku.events
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "handle" should {

    "decode an event from the request, " +
      "schedule commit synchronization " +
      s"and return $Accepted - full commit sync event case" in new TestCase {

        val event = fullCommitSyncEvents.generateOne

        (commitsSynchronizer.synchronizeEvents _)
          .expects(event)
          .returning(().pure[IO])

        handler
          .createHandlingProcess(requestContent(event.asJson))
          .unsafeRunSync()
          .process
          .value
          .unsafeRunSync() shouldBe Right(
          Accepted
        )

        logger.loggedOnly(Info(s"${logMessageCommon(event)} -> $Accepted"))
      }

    "decode an event from the request, " +
      "schedule commit synchronization " +
      s"and return $Accepted - minimal commit sync event case" in new TestCase {

        val event = minimalCommitSyncEvents.generateOne

        (commitsSynchronizer.synchronizeEvents _)
          .expects(event)
          .returning(().pure[IO])

        handler
          .createHandlingProcess(requestContent(event.asJson))
          .unsafeRunSync()
          .process
          .value
          .unsafeRunSync() shouldBe Right(
          Accepted
        )

        logger.loggedOnly(Info(s"${logMessageCommon(event)} -> $Accepted"))
      }

    s"return $Accepted and log an error if scheduling event synchronization fails" in new TestCase {

      val event = commitSyncEvents.generateOne

      (commitsSynchronizer.synchronizeEvents _)
        .expects(event)
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      handler
        .createHandlingProcess(requestContent(event.asJson))
        .unsafeRunSync()
        .process
        .value
        .unsafeRunSync() shouldBe Right(Accepted)

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

      handler.createHandlingProcess(request).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger()
    val commitsSynchronizer = mock[CommitsSynchronizer[IO]]
    val handler             = new EventHandler[IO](categoryName, commitsSynchronizer)

    def requestContent(event: Json): EventRequestContent = events.EventRequestContent.NoPayload(event)
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
    case MinimalCommitSyncEvent(project) => json"""{
        "categoryName": "COMMIT_SYNC",
        "project": {
          "id":         ${project.id.value},
          "path":       ${project.path.value}
        }
      }"""
  }
}
