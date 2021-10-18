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

package io.renku.commiteventservice.events.categories.globalcommitsync

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import io.renku.commiteventservice.events.categories.globalcommitsync.Generators.globalCommitSyncEventsNonZero
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer
import ch.datascience.events.EventRequestContent
import ch.datascience.events.consumers.ConcurrentProcessesLimiter
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, jsons}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.tinytypes.json.TinyTypeEncoders._
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

        val event = globalCommitSyncEventsNonZero.generateOne

        (commitEventSynchronizer.synchronizeEvents _)
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

        val event = globalCommitSyncEventsNonZero.generateOne

        (commitEventSynchronizer.synchronizeEvents _)
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

      val event = globalCommitSyncEventsNonZero.generateOne

      (commitEventSynchronizer.synchronizeEvents _)
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

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val commitEventSynchronizer    = mock[GlobalCommitEventSynchronizer[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val logger                     = TestLogger[IO]()

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)

    val handler =
      new EventHandler[IO](categoryName,
                           commitEventSynchronizer,
                           subscriptionMechanism,
                           concurrentProcessesLimiter,
                           logger
      )

    def requestContent(event: Json): EventRequestContent = EventRequestContent.NoPayload(event)
  }

  private implicit def eventEncoder[E <: GlobalCommitSyncEvent]: Encoder[E] = Encoder.instance[E] {
    case GlobalCommitSyncEvent(project, commitIds) => json"""{
        "categoryName": "GLOBAL_COMMIT_SYNC",
        "project": {
          "id":         ${project.id.value},
          "path":       ${project.path.value}
        },
        "commits":    ${Json.arr(commitIds.map(_.asJson): _*)}
      }"""
  }

}
