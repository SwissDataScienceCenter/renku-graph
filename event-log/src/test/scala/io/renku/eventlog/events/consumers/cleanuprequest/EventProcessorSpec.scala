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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.TryValues

class EventProcessorSpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory with TryValues {

  "process" should {

    "offer project id and slug to the queue if a Full event is given" in new TestCase {
      val projectId   = projectIds.generateOne
      val projectSlug = projectSlugs.generateOne

      (eventsQueue.offer _).expects(projectId, projectSlug).returning(().pure[IO])

      processor.process(CleanUpRequestEvent(projectId, projectSlug)).unsafeRunSync() shouldBe ()
    }

    "offer project id and slug to the queue if a Partial event is given " +
      "but projectId was found in the project table" in new TestCase {
        val projectId   = projectIds.generateOne
        val projectSlug = projectSlugs.generateOne

        (projectIdFinder.findProjectId _).expects(projectSlug).returning(projectId.some.pure[IO])

        (eventsQueue.offer _).expects(projectId, projectSlug).returning(().pure[IO])

        processor.process(CleanUpRequestEvent(projectSlug)).unsafeRunSync() shouldBe ()
      }

    "log a warning for a Partial event when projectId cannot be found in the project table" in new TestCase {
      val projectSlug = projectSlugs.generateOne

      (projectIdFinder.findProjectId _).expects(projectSlug).returning(Option.empty[projects.GitLabId].pure[IO])

      processor.process(CleanUpRequestEvent(projectSlug)).unsafeRunSync() shouldBe ()
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger()
    val projectIdFinder = mock[ProjectIdFinder[IO]]
    val eventsQueue     = mock[CleanUpEventsQueue[IO]]
    val processor       = new EventProcessorImpl(projectIdFinder, eventsQueue)
  }
}
