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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class EventProcessorSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "process" should {

    "offer project id and path to the queue if a Full event is given" in new TestCase {
      val projectId   = projectIds.generateOne
      val projectPath = projectPaths.generateOne

      (eventsQueue.offer _).expects(projectId, projectPath).returning(().pure[Try])

      processor.process(CleanUpRequestEvent(projectId, projectPath)) shouldBe ().pure[Try]
    }

    "offer project id and path to the queue if a Partial event is given " +
      "but projectId was found in the project table" in new TestCase {
        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        (projectIdFinder.findProjectId _).expects(projectPath).returning(projectId.some.pure[Try])

        (eventsQueue.offer _).expects(projectId, projectPath).returning(().pure[Try])

        processor.process(CleanUpRequestEvent(projectPath)) shouldBe ().pure[Try]
      }

    "fail for a Partial event when projectId cannot be found in the project table" in new TestCase {
      val projectPath = projectPaths.generateOne

      (projectIdFinder.findProjectId _).expects(projectPath).returning(Option.empty[projects.Id].pure[Try])

      val Failure(exception) = processor.process(CleanUpRequestEvent(projectPath))

      exception.getMessage shouldBe show"Cannot find projectId for $projectPath"
    }
  }

  private trait TestCase {
    val projectIdFinder = mock[ProjectIdFinder[Try]]
    val eventsQueue     = mock[CleanUpEventsQueue[Try]]
    val processor       = new EventProcessorImpl(projectIdFinder, eventsQueue)
  }
}
