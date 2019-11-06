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

package ch.datascience.triplesgenerator.reprovisioning.postreprovisioning

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info, Warn}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.triplesgenerator.reprovisioning.RdfStoreUpdater
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class PostReProvisioningSpec extends WordSpec with MockFactory {

  "run" should {

    "execute all the post re-processing steps and succeed if no errors" in new TestCase {

      inSequence {
        steps foreach { step =>
          (step.run _)
            .expects()
            .returning(context.unit)
        }
      }

      postReProvisioning.run shouldBe context.unit

      logger.loggedOnly(
        (steps map executionTimeWarning) :+ Info("Post re-provisioning finished")
      )
    }

    "log an error for a failing step and execute all the other steps" in new TestCase {

      steps foreach { failingStep =>
        val exception = exceptions.generateOne
        (failingStep.run _)
          .expects()
          .returning(context raiseError exception)

        steps.filterNot(_ == failingStep) foreach { nonFailingStep =>
          (nonFailingStep.run _)
            .expects()
            .returning(context.unit)
        }

        postReProvisioning.run shouldBe context.unit

        logger.loggedOnly(
          (steps map executionTimeWarning) :+
            Error(s"${failingStep.description} failed", exception) :+
            Info("Post re-provisioning finished")
        )
        logger.reset()
      }
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val orphanProjectsRemover      = mock[OrphanProjectsRemover[Try]]
    val orphanPersonsRemover       = mock[OrphanPersonsRemover[Try]]
    val orphanMailtoNoneRemover    = mock[OrphanMailtoNoneRemover[Try]]
    val mailtoEmailRemover         = mock[MailtoEmailRemover[Try]]
    val duplicatePersonNameRemover = mock[DuplicatePersonNameRemover[Try]]
    val orphanAgentsRemover        = mock[OrphanAgentsRemover[Try]]
    val logger                     = TestLogger[Try]()
    val executionTimeRecorder      = TestExecutionTimeRecorder(logger)
    val postReProvisioning = new PostReProvisioning[Try](
      orphanProjectsRemover,
      orphanPersonsRemover,
      orphanMailtoNoneRemover,
      mailtoEmailRemover,
      duplicatePersonNameRemover,
      orphanAgentsRemover,
      executionTimeRecorder,
      logger
    )

    val steps: List[RdfStoreUpdater[Try]] = List(
      orphanProjectsRemover,
      orphanPersonsRemover,
      orphanMailtoNoneRemover,
      mailtoEmailRemover,
      duplicatePersonNameRemover,
      orphanAgentsRemover
    )

    def executionTimeWarning(step: RdfStoreUpdater[Try]) =
      Warn(s"${step.description} executed${executionTimeRecorder.executionTimeInfo}")
  }
}
