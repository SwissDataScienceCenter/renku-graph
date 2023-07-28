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

package io.renku.commiteventservice.events.consumers.globalcommitsync

import Generators._
import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.syntax._
import io.renku.commiteventservice.events.consumers.globalcommitsync.eventgeneration.CommitsSynchronizer
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "handlingDefinition.decode" should {

    "decode the event from the request" in new TestCase {

      val event = globalCommitSyncEvents().generateOne

      handler
        .createHandlingDefinition()
        .decode(requestContent(event.asJson)) shouldBe event.asRight
    }
  }

  "handlingDefinition.process" should {

    "be the synchronizer.synchronizeEvents" in new TestCase {

      val event = globalCommitSyncEvents().generateOne

      (synchronizer.synchronizeEvents _)
        .expects(event)
        .returning(().pure[IO])

      handler
        .createHandlingDefinition()
        .process(event)
        .unsafeRunSync() shouldBe ()
    }
  }

  "createHandlingDefinition" should {

    "not define precondition" in new TestCase {
      handler.createHandlingDefinition().precondition shouldBe None.pure[IO]
    }
  }

  "handlingDefinition.onRelease" should {

    "do the renewSubscription" in new TestCase {

      handler.createHandlingDefinition().onRelease.foreach(_.unsafeRunSync())

      renewSubscriptionCalled.get.unsafeRunSync() shouldBe true
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val synchronizer                  = mock[CommitsSynchronizer[IO]]
    private val subscriptionMechanism = mock[SubscriptionMechanism[IO]]
    val renewSubscriptionCalled       = Ref.unsafe[IO, Boolean](false)
    (subscriptionMechanism.renewSubscription _).expects().returns(renewSubscriptionCalled.set(true))

    val handler = new EventHandler[IO](categoryName, synchronizer, subscriptionMechanism, mock[ProcessExecutor[IO]])

    def requestContent(event: Json): EventRequestContent = EventRequestContent.NoPayload(event)
  }

  private implicit lazy val eventEncoder: Encoder[GlobalCommitSyncEvent] = Encoder.instance {
    case GlobalCommitSyncEvent(project, commits) => json"""{
      "categoryName": "GLOBAL_COMMIT_SYNC",
      "project": {
        "id":   ${project.id},
        "slug": ${project.slug}
      },
      "commits": {
        "count":  ${commits.count},
        "latest": ${commits.latest}
      }
    }"""
  }
}
