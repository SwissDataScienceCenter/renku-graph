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

package io.renku.eventlog.events.consumers
package projectsync

import cats.effect.IO
import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.events.consumers.ConcurrentProcessesLimiter
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
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
      "kick-off the project synchronization process " +
      s"and return $Accepted" in new TestCase {

        (synchronizer.syncProjectInfo _).expects(event).returning(().pure[IO])

        handler
          .createHandlingProcess(requestContent(event.asJson))
          .unsafeRunSync()
          .process
          .value
          .unsafeRunSync() shouldBe Accepted.asRight

        logger.loggedOnly(Info(show"$categoryName: $event -> $Accepted"))
      }

    s"return $Accepted when synchronization process fails" in new TestCase {

      (synchronizer.syncProjectInfo _).expects(event).returning(exceptions.generateOne.raiseError[IO, Unit])

      handler
        .createHandlingProcess(requestContent(event.asJson))
        .unsafeRunSync()
        .process
        .value
        .unsafeRunSync() shouldBe Accepted.asRight

      logger.loggedOnly(Info(show"$categoryName: $event -> $Accepted"))
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": ${categoryName.show}
        }"""
      }

      handler.createHandlingProcess(request).unsafeRunSync().process.value.unsafeRunSync() shouldBe BadRequest.asLeft

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    val event = projectSyncEvents.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val synchronizer               = mock[ProjectInfoSynchronizer[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val handler = new EventHandler[IO](categoryName, synchronizer, subscriptionMechanism, concurrentProcessesLimiter)

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)
  }

  private implicit lazy val eventEncoder: Encoder[ProjectSyncEvent] = Encoder.instance[ProjectSyncEvent] {
    case ProjectSyncEvent(id, path) => json"""{
      "categoryName": "PROJECT_SYNC",
      "project": {
        "id":   ${id.value},
        "path": ${path.value}
      }
    }"""
  }

  private lazy val projectSyncEvents = for {
    id   <- projectIds
    path <- projectPaths
  } yield ProjectSyncEvent(id, path)
}
