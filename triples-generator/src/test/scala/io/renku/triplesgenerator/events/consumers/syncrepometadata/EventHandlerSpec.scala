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
import io.circe.syntax._
import io.renku.events.EventRequestContent
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
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class EventHandlerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with should.Matchers
    with EitherValues
    with AsyncMockFactory {

  "handlingDefinition.decode" should {

    "be EventDecoder.decode" in {

      expectTSReadinessCheckerCall

      val event = syncRepoMetadataEvents.generateOne

      handler(noLock)
        .createHandlingDefinition()
        .decode(EventRequestContent.NoPayload(event.asJson))
        .value shouldBe event
    }
  }

  "handlingDefinition.process" should {

    "be the EventProcessor.process" in {

      expectTSReadinessCheckerCall

      val event = syncRepoMetadataEvents.generateOne

      (eventProcessor.process _).expects(event).returns(().pure[IO])

      handler(noLock).createHandlingDefinition().process(event).assertNoException
    }

    "lock while executing" in {

      val test = Ref.unsafe[IO, Int](0)
      val tsWriteLock: TsWriteLock[IO] =
        Lock.from[IO, projects.Slug](Kleisli(_ => test.update(_ + 1)))(Kleisli(_ => test.update(_ + 1)))

      expectTSReadinessCheckerCall

      val event = syncRepoMetadataEvents.generateOne

      (eventProcessor.process _).expects(event).returns(().pure[IO])

      handler(tsWriteLock).createHandlingDefinition().process(event).assertNoException >>
        test.get.asserting(_ shouldBe 2)
    }
  }

  "handlingDefinition.precondition" should {

    "be the TSReadinessForEventsChecker.verifyTSReady" in {

      expectTSReadinessCheckerCall

      handler(noLock).createHandlingDefinition().precondition.asserting(_ shouldBe readinessCheckerResult)
    }
  }

  "handlingDefinition.onRelease" should {

    "not be defined" in {

      expectTSReadinessCheckerCall

      handler(noLock).createHandlingDefinition().onRelease shouldBe None
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()

  private lazy val tsReadinessChecker     = mock[TSReadinessForEventsChecker[IO]]
  private lazy val readinessCheckerResult = eventSchedulingResults.generateSome

  private def expectTSReadinessCheckerCall =
    (() => tsReadinessChecker.verifyTSReady).expects().returns(readinessCheckerResult.pure[IO])

  private lazy val eventProcessor = mock[EventProcessor[IO]]

  private lazy val noLock = Lock.none[IO, projects.Slug]

  private def handler(tsWriteLock: TsWriteLock[IO]) =
    new EventHandler[IO](categoryName, tsReadinessChecker, eventProcessor, mock[ProcessExecutor[IO]], tsWriteLock)
}
