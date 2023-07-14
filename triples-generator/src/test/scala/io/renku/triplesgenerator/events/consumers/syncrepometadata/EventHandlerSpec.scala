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

package io.renku.triplesgenerator.events.consumers.syncrepometadata

import cats.data.Kleisli
import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.events.consumers.ConsumersModelGenerators.eventSchedulingResults
import io.renku.events.consumers.ProcessExecutor
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.lock.Lock
import io.renku.triplesgenerator.TgLockDB.TsWriteLock
import io.renku.triplesgenerator.api.events.Generators.syncRepoMetadataEvents
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import io.renku.triplesgenerator.events.consumers.syncrepometadata.processor.EventProcessor
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class EventHandlerSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  "handlingDefinition.decode" should {

    "be EventDecoder.decode" in {
      val tc = new TestCase

      tc.expectTSReadinessCheckerCall

      tc.handler.createHandlingDefinition().decode shouldBe EventDecoder.decode
    }
  }

  "handlingDefinition.process" should {

    "be the EventProcessor.process" ignore {
      val tc = new TestCase
      tc.expectTSReadinessCheckerCall

      syncRepoMetadataEvents[IO].map(_.generateOne).flatMap { event =>
        (tc.eventProcessor.process _).expects(event).returns(().pure[IO])

        tc.handler.createHandlingDefinition().process(event).assertNoException
      }
    }

    "lock while executing" ignore {
      val test = Ref.unsafe[IO, Int](0)
      val tc = new TestCase {
        override val tsWriteLock: TsWriteLock[IO] =
          Lock.from[IO, projects.Path](Kleisli(_ => test.update(_ + 1)))(Kleisli(_ => test.update(_ + 1)))
      }

      tc.expectTSReadinessCheckerCall

      syncRepoMetadataEvents[IO].map(_.generateOne).flatMap { event =>
        (tc.eventProcessor.process _).expects(event).returns(().pure[IO])

        tc.handler.createHandlingDefinition().process(event).map { r =>
          r                        shouldBe ()
          test.get.unsafeRunSync() shouldBe 2
        }
      }
    }
  }

  "handlingDefinition.precondition" should {

    "be the TSReadinessForEventsChecker.verifyTSReady" in {
      val tc = new TestCase
      tc.expectTSReadinessCheckerCall

      tc.handler.createHandlingDefinition().precondition.asserting(_ shouldBe tc.readinessCheckerResult)
    }
  }

  "handlingDefinition.onRelease" should {

    "not be defined" in {
      val tc = new TestCase
      tc.expectTSReadinessCheckerCall

      tc.handler.createHandlingDefinition().onRelease shouldBe None
    }
  }

  class TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    lazy val tsReadinessChecker     = mock[TSReadinessForEventsChecker[IO]]
    lazy val readinessCheckerResult = eventSchedulingResults.generateSome

    def expectTSReadinessCheckerCall =
      (() => tsReadinessChecker.verifyTSReady).expects().returns(readinessCheckerResult.pure[IO])

    lazy val eventProcessor = mock[EventProcessor[IO]]

    def tsWriteLock: TsWriteLock[IO] = Lock.none[IO, projects.Path]

    lazy val handler =
      new EventHandler[IO](
        categoryName,
        tsReadinessChecker,
        EventDecoder,
        eventProcessor,
        mock[ProcessExecutor[IO]],
        tsWriteLock
      )
  }
}
