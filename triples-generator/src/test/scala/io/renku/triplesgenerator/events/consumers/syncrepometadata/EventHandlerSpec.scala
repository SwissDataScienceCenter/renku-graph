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

import cats.effect.IO
import cats.syntax.all._
import io.renku.events.consumers.ConsumersModelGenerators.eventSchedulingResults
import io.renku.events.consumers.ProcessExecutor
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators.syncRepoMetadataEvents
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "handlingDefinition.decode" should {

    "be EventDecoder.decode" in new TestCase {
      handler.createHandlingDefinition().decode shouldBe EventDecoder.decode
    }
  }

  "handlingDefinition.process" should {

    "be the EventProcessor.process" in new TestCase {

      val event = syncRepoMetadataEvents.generateOne

      (eventProcessor.process _).expects(event).returns(().pure[IO])

      handler.createHandlingDefinition().process(event).unsafeRunSync() shouldBe ()
    }
  }

  "handlingDefinition.precondition" should {

    "be the TSReadinessForEventsChecker.verifyTSReady" in new TestCase {
      handler.createHandlingDefinition().precondition.unsafeRunSync() shouldBe readinessCheckerResult
    }
  }

  "handlingDefinition.onRelease" should {

    "not be defined" in new TestCase {
      handler.createHandlingDefinition().onRelease shouldBe None
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    private val tsReadinessChecker = mock[TSReadinessForEventsChecker[IO]]
    val readinessCheckerResult     = eventSchedulingResults.generateSome
    (() => tsReadinessChecker.verifyTSReady).expects().returns(readinessCheckerResult.pure[IO])

    val eventProcessor = mock[EventProcessor[IO]]

    val handler =
      new EventHandler[IO](categoryName, tsReadinessChecker, EventDecoder, eventProcessor, mock[ProcessExecutor[IO]])
  }
}
