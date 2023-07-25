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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.data.Kleisli
import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{DecodingFailure, Encoder}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ConsumersModelGenerators.eventSchedulingResults
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.jsons
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.lock.Lock
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.TgLockDB.TsWriteLock
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers with EitherValues {

  "handlingDefinition.decode" should {

    "decode the Project path from the event" in new TestCase {
      handler
        .createHandlingDefinition()
        .decode(EventRequestContent.NoPayload(event))
        .value shouldBe eventProject
    }

    "fail for malformed json" in new TestCase {
      handler
        .createHandlingDefinition()
        .decode(EventRequestContent.NoPayload(jsons.generateOne))
        .left
        .value shouldBe a[DecodingFailure]
    }
  }

  "handlingDefinition.process" should {

    "be the MembersSynchronizer.synchronizerMembers" in new TestCase {

      (membersSynchronizer.synchronizeMembers _).expects(eventProject).returns(().pure[IO])

      handler.createHandlingDefinition().process(eventProject).unsafeRunSync() shouldBe ()
    }

    "lock while executing" in new TestCase {
      val test = Ref.unsafe[IO, Int](0)
      override val tsWriteLock: TsWriteLock[IO] =
        Lock.from[IO, projects.Path](Kleisli(_ => test.update(_ + 1)))(Kleisli(_ => test.update(_ + 1)))

      (membersSynchronizer.synchronizeMembers _).expects(eventProject).returns(().pure[IO])

      handler.createHandlingDefinition().process(eventProject).unsafeRunSync() shouldBe ()
      test.get.unsafeRunSync()                                                 shouldBe 2
    }
  }

  "handlingDefinition.precondition" should {

    "be the TSReadinessForEventsChecker.verifyTSReady" in new TestCase {
      handler.createHandlingDefinition().precondition.unsafeRunSync() shouldBe readinessCheckerResult
    }
  }

  "handlingDefinition.onRelease" should {

    "do the renewSubscription" in new TestCase {

      handler.createHandlingDefinition().onRelease.foreach(_.unsafeRunSync())

      renewSubscriptionCalled.get.unsafeRunSync() shouldBe true
    }
  }

  private trait TestCase {

    val eventProject = projectPaths.generateOne
    val event        = eventProject.asJson(eventEncoder)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val tsReadinessChecker = mock[TSReadinessForEventsChecker[IO]]
    val readinessCheckerResult     = eventSchedulingResults.generateSome
    (() => tsReadinessChecker.verifyTSReady).expects().returns(readinessCheckerResult.pure[IO])

    val membersSynchronizer = mock[MembersSynchronizer[IO]]

    private val subscriptionMechanism = mock[SubscriptionMechanism[IO]]
    val renewSubscriptionCalled       = Ref.unsafe[IO, Boolean](false)
    (subscriptionMechanism.renewSubscription _).expects().returns(renewSubscriptionCalled.set(true))

    def tsWriteLock: TsWriteLock[IO] = Lock.none[IO, projects.Path]
    lazy val handler = new EventHandler[IO](
      categoryName,
      tsReadinessChecker,
      membersSynchronizer,
      subscriptionMechanism,
      mock[ProcessExecutor[IO]],
      tsWriteLock
    )
  }

  private lazy val eventEncoder: Encoder[projects.Path] = Encoder.instance { projectPath =>
    json"""{
      "categoryName": "MEMBER_SYNC",
      "project": {
        "path": $projectPath
      }
    }"""
  }
}
