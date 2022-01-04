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

package io.renku.commiteventservice.events.categories.globalcommitsync

import Generators._
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.CommitsSynchronizer
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ConcurrentProcessesLimiter
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons}
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
      s"schedule commit synchronization and return $Accepted" in new TestCase {

        val event = globalCommitSyncEvents().generateOne

        (commitEventSynchronizer.synchronizeEvents _)
          .expects(event)
          .returning(().pure[IO])

        handler
          .createHandlingProcess(requestContent(event.asJson))
          .unsafeRunSync()
          .process
          .value
          .unsafeRunSync() shouldBe Accepted.asRight

        logger.loggedOnly(Info(s"${logMessageCommon(event)} -> $Accepted"))
      }

    s"return $Accepted and log an error if scheduling event synchronization fails" in new TestCase {

      val event = globalCommitSyncEvents().generateOne

      (commitEventSynchronizer.synchronizeEvents _)
        .expects(event)
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      handler
        .createHandlingProcess(requestContent(event.asJson))
        .unsafeRunSync()
        .process
        .value
        .unsafeRunSync() shouldBe Accepted.asRight

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

      handler.createHandlingProcess(request).unsafeRunSync().process.value.unsafeRunSync() shouldBe BadRequest.asLeft

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val commitEventSynchronizer    = mock[CommitsSynchronizer[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)

    val handler =
      new EventHandler[IO](categoryName, commitEventSynchronizer, subscriptionMechanism, concurrentProcessesLimiter)

    def requestContent(event: Json): EventRequestContent = EventRequestContent.NoPayload(event)
  }

  private implicit def eventEncoder[E <: GlobalCommitSyncEvent]: Encoder[E] = Encoder.instance[E] {
    case GlobalCommitSyncEvent(project, commits) => json"""{
      "categoryName": "GLOBAL_COMMIT_SYNC",
      "project": {
        "id":         ${project.id.value},
        "path":       ${project.path.value}
      },
      "commits": {
        "count":  ${commits.count.value},
        "latest": ${commits.latest.value}
      }
    }"""
  }
}
