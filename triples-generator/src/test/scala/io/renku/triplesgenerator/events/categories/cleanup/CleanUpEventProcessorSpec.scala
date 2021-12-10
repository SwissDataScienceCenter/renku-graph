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

package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.IO
import cats.syntax.all._
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.EventStatusUpdater
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CleanUpEventProcessorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {
  "processEvent" should {
    "remove the triples linked to the project and notify the eventlog when the process is done" in new TestCase {
      (triplesRemover.removeTriples _).expects(path).returns(().pure[IO])
      (eventStatusUpdater.projectToNew _).expects(project).returns(().pure[IO])
      cleanUpEventProcessor.process(project).unsafeRunSync() shouldBe ()
    }
    "fail if the removal of the triples fail and log the error" in new TestCase {
      val exception = exceptions.generateOne
      (triplesRemover.removeTriples _).expects(path).returns(exception.raiseError[IO, Unit])
      intercept[Exception] {
        cleanUpEventProcessor.process(project).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
      eventually {
        logger.loggedOnly(
          Error(s"${commonLogMessage(project)} - Triples removal failed ${exception.getMessage}", exception)
        )
      }
    }
    "fail if the notification of the eventlog fails and log the error" in new TestCase {
      val exception = exceptions.generateOne
      (triplesRemover.removeTriples _).expects(path).returns(().pure[IO])
      (eventStatusUpdater.projectToNew _).expects(project).returns(exception.raiseError[IO, Unit])
      intercept[Exception] {
        cleanUpEventProcessor.process(project).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
      eventually {
        logger.loggedOnly(
          Error(
            s"${commonLogMessage(project)} - Triples removal, event log notification failed ${exception.getMessage}",
            exception
          )
        )
      }
    }
  }
  private def commonLogMessage(project: Project): String =
    s"$categoryName: ${project.show}"

  private trait TestCase {
    val path:    projects.Path = projectPaths.generateOne
    val project: Project       = Project(projectIds.generateOne, path)

    implicit val logger = TestLogger[IO]()

    val triplesRemover     = mock[ProjectTriplesRemover[IO]]
    val eventStatusUpdater = mock[EventStatusUpdater[IO]]

    val cleanUpEventProcessor = new CleanUpEventProcessorImpl[IO](triplesRemover, eventStatusUpdater)
  }
}
