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

package io.renku.webhookservice

import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.model.CommitSyncRequest
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class CommitSyncRequestSenderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "send" should {

    s"succeed when delivering the event to the Event Log got $Accepted" in new TestCase {

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          EventRequestContent.NoPayload(syncRequest.asJson),
          EventSender.EventContext(CategoryName("COMMIT_SYNC_REQUEST"),
                                   show"$processName - sending COMMIT_SYNC_REQUEST for ${syncRequest.project} failed"
          )
        )
        .returning(().pure[Try])

      requestSender.sendCommitSyncRequest(syncRequest, processName) shouldBe ().pure[Try]

      logger.loggedOnly(Info(show"$processName - COMMIT_SYNC_REQUEST sent for ${syncRequest.project}"))
    }

    "fail when sending the event fails" in new TestCase {
      val exception = exceptions.generateOne
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          EventRequestContent.NoPayload(syncRequest.asJson),
          EventSender.EventContext(CategoryName("COMMIT_SYNC_REQUEST"),
                                   show"$processName - sending COMMIT_SYNC_REQUEST for ${syncRequest.project} failed"
          )
        )
        .returning(exception.raiseError[Try, Unit])

      requestSender.sendCommitSyncRequest(syncRequest, processName) shouldBe exception.raiseError[Try, Unit]
    }
  }

  private trait TestCase {

    val processName = nonEmptyStrings().generateOne
    val syncRequest = commitSyncRequests.generateOne

    val eventSender = mock[EventSender[Try]]
    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val requestSender = new CommitSyncRequestSenderImpl[Try](eventSender)
  }

  private implicit lazy val eventEncoder: Encoder[CommitSyncRequest] = Encoder.instance[CommitSyncRequest] { event =>
    json"""{
      "categoryName": "COMMIT_SYNC_REQUEST",
      "project": {
        "id":      ${event.project.id.value},
        "path":    ${event.project.path.value}
      }
    }"""
  }
}
