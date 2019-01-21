/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.queues.pushevent

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.{ActorMaterializer, QueueOfferResult}
import ch.datascience.config.{AsyncParallelism, BufferSize}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events._
import ch.datascience.tools.AsyncTestCase
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.queues.commitevent.CommitEventsQueue
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import play.api.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class PushEventQueueSpec extends WordSpec with MockFactory with Eventually with ScalaFutures with IntegrationPatience {

  "offer" should {

    "return Enqueued and trigger conversion to CommitEvent and offer to the CommitEvent queue" in new TestCase {
      val pushEvent:   PushEvent   = pushEvents.generateOne
      val commitEvent: CommitEvent = toCommitEvent(pushEvent)

      givenCommitEventOffer(commitEvent) returningAndFinishingTest Future(Enqueued)

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      waitForAsyncProcess
    }

    "not kill the queue when multiple events are offered and some of them fail" in new TestCase {
      val pushEvent1:   PushEvent   = pushEvents.generateOne
      val commitEvent1: CommitEvent = toCommitEvent(pushEvent1)
      givenCommitEventOffer(commitEvent1) returning Future(Enqueued)

      val pushEvent2:   PushEvent   = pushEvents.generateOne
      val commitEvent2: CommitEvent = toCommitEvent(pushEvent2)
      val exception2 = exceptions.generateOne
      givenCommitEventOffer(commitEvent2) returning Future.failed(exception2)

      val pushEvent3:   PushEvent   = pushEvents.generateOne
      val commitEvent3: CommitEvent = toCommitEvent(pushEvent3)
      givenCommitEventOffer(commitEvent3) returningAndFinishingTest Future(Enqueued)

      pushEventQueue.offer(pushEvent1).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent2).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent3).futureValue shouldBe Enqueued

      waitForAsyncProcess
    }
  }

  private trait TestCase extends AsyncTestCase {
    private implicit val system       = ActorSystem("MyTest")
    private implicit val materializer = ActorMaterializer()

    val commitsEventQueue = mock[CommitEventsQueue]
    val pushEventQueue = new PushEventQueue(
      QueueConfig(
        bufferSize               = BufferSize(1),
        commitDetailsParallelism = AsyncParallelism(1)
      ),
      commitsEventQueue,
      Logger
    )

    def givenCommitEventOffer(commitEvent: CommitEvent) = new {
      private val stubbing =
        (commitsEventQueue
          .offer(_: CommitEvent))
          .expects(commitEvent)

      def returning(outcome: Future[QueueOfferResult]): Unit =
        stubbing.returning(outcome)

      def returningAndFinishingTest(outcome: Future[QueueOfferResult]): Unit =
        stubbing
          .returning(outcome)
          .onCall { _: CommitEvent =>
            asyncProcessFinished()
            outcome
          }
    }

    def toCommitEvent(pushEvent: PushEvent) = CommitEvent(
      pushEvent.after,
      "",
      Instant.EPOCH,
      pushEvent.pushUser,
      author    = User(pushEvent.pushUser.username, pushEvent.pushUser.email),
      committer = User(pushEvent.pushUser.username, pushEvent.pushUser.email),
      parents   = Seq(pushEvent.before),
      project   = pushEvent.project,
      added     = Nil,
      modified  = Nil,
      removed   = Nil
    )
  }
}
