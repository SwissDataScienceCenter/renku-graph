/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.cleanup

import cats.effect.IO
import cats.syntax.all._
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventProcessorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "processEvent" should {

    "remove the triples linked to the project and notify the EL when the process is done" in new TestCase {

      (tsCleaner.removeTriples _).expects(project.slug).returns(().pure[IO])

      (eventStatusUpdater.projectToNew _).expects(project).returns(().pure[IO])

      (eventProcessor process project).unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info(s"${commonLogMessage(project)} accepted"))
    }

    "do not fail but log an error if the removal of the triples fails" in new TestCase {

      val exception = exceptions.generateOne
      (tsCleaner.removeTriples _).expects(project.slug).returns(exception.raiseError[IO, Unit])

      (eventProcessor process project).unsafeRunSync() shouldBe ()

      logger.logged(Error(s"${commonLogMessage(project)} triples removal failure", exception))
    }

    "do not fail but log an error if the notification of the EL fails" in new TestCase {

      (tsCleaner.removeTriples _).expects(project.slug).returns(().pure[IO])

      val exception = exceptions.generateOne
      (eventStatusUpdater.projectToNew _).expects(project).returns(exception.raiseError[IO, Unit])

      (eventProcessor process project).unsafeRunSync() shouldBe ()

      logger.logged(Error(s"${commonLogMessage(project)} triples removal failure", exception))
    }
  }

  private def commonLogMessage(project: Project): String = show"$categoryName: $project"

  private trait TestCase {

    val project = consumerProjects.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tsCleaner          = mock[namedgraphs.TSCleaner[IO]]
    val eventStatusUpdater = mock[EventStatusUpdater[IO]]
    val eventProcessor     = new EventProcessorImpl[IO](tsCleaner, eventStatusUpdater)
  }
}
