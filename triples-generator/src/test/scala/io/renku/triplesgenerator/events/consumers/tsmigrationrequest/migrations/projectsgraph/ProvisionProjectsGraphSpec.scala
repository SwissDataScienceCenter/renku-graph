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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations
package projectsgraph

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.projectPaths
import cats.MonadThrow
import io.renku.generators.Generators.{exceptions, nonEmptyStrings, positiveInts}
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class ProvisionProjectsGraphSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with MockFactory
    with EitherValues {

  "ProvisionProjectsGraph" should {

    "be the ConditionedMigration" in new TestCase {
      migration.getClass.getSuperclass shouldBe classOf[ConditionedMigration[IO]]
    }
  }

  "required" should {

    Set(
      ConditionedMigration.MigrationRequired.No(nonEmptyStrings().generateOne),
      ConditionedMigration.MigrationRequired.Yes(nonEmptyStrings().generateOne)
    ) foreach { answer =>
      s"return $answer if there are no projects to migrate" in new TestCase {

        (() => migrationNeedChecker.checkMigrationNeeded)
          .expects()
          .returning(answer.pure[IO])

        migration.required.value.unsafeRunSync().value shouldBe answer
      }
    }
  }

  "migrate" should {

    "prepare backlog of projects requiring provisioning " +
      "and start the process that " +
      "goes through all the prepared projects, " +
      "send a RedoProjectTransformation event to EL for each of them " +
      "and keep a note of the project once an event for it is sent" in new TestCase {

        givenBacklogCreated()

        val allProjects = projectPaths.generateList(min = pageSize, max = pageSize * 2)

        val projectsPages = allProjects
          .sliding(pageSize, pageSize)
          .toList
        givenProjectsPagesReturned(projectsPages :+ List.empty[projects.Path])

        givenProgressInfoFinding(returning = nonEmptyStrings().generateOne.pure[IO], times = allProjects.size)

        allProjects map toRedoProjectTransformation foreach verifyEventWasSent

        allProjects foreach verifyProjectNotedDone

        migration.migrate().value.unsafeRunSync().value shouldBe ()
      }

    "return a Recoverable Error in case the recovery strategy returns one while finding projects" in new TestCase {

      givenBacklogCreated()

      val exception = exceptions.generateOne
      (() => projectsFinder.nextProjectsPage())
        .expects()
        .returning(exception.raiseError[IO, List[projects.Path]])

      migration.migrate().value.unsafeRunSync().left.value shouldBe recoverableError
    }
  }

  "postMigration" should {

    "register migration execution" in new TestCase {

      verifyMigrationExecutionRegistered

      migration.postMigration().value.unsafeRunSync().value shouldBe ()
    }
  }

  private trait TestCase {
    val pageSize = positiveInts(max = 100).generateOne.value

    private val eventCategoryName = CategoryName("EVENTS_STATUS_CHANGE")
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val migrationNeedChecker         = mock[MigrationNeedChecker[IO]]
    private val backlogCreator       = mock[BacklogCreator[IO]]
    val projectsFinder               = mock[ProjectsPageFinder[IO]]
    private val progressFinder       = mock[ProgressFinder[IO]]
    private val eventSender          = mock[EventSender[IO]]
    private val projectDonePersister = mock[ProjectDonePersister[IO]]
    private val executionRegister    = mock[MigrationExecutionRegister[IO]]
    val recoverableError             = processingRecoverableErrors.generateOne
    private val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new ProvisionProjectsGraph[IO](
      migrationNeedChecker,
      backlogCreator,
      projectsFinder,
      progressFinder,
      eventSender,
      projectDonePersister,
      executionRegister,
      recoveryStrategy
    )

    def givenBacklogCreated() =
      (backlogCreator.createBacklog _)
        .expects()
        .returning(().pure[IO])

    def givenProjectsPagesReturned(pages: List[List[projects.Path]]): Unit =
      pages foreach { page =>
        (projectsFinder.nextProjectsPage _)
          .expects()
          .returning(page.pure[IO])
      }

    def givenProgressInfoFinding(returning: IO[String], times: Int) =
      (() => progressFinder.findProgressInfo)
        .expects()
        .returning(returning)
        .repeat(times)

    def toRedoProjectTransformation(projectPath: projects.Path) = projectPath -> EventRequestContent.NoPayload {
      json"""{
        "categoryName": $eventCategoryName,
        "project": {
          "path": $projectPath
        }
      }"""
    }

    lazy val verifyEventWasSent: ((projects.Path, EventRequestContent.NoPayload)) => Unit = { case (path, event) =>
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(event,
                 EventSender.EventContext(eventCategoryName,
                                          show"$categoryName: ${migration.name} cannot send event for $path"
                 )
        )
        .returning(().pure[IO])
      ()
    }

    def verifyProjectNotedDone(path: projects.Path) =
      (projectDonePersister.noteDone _)
        .expects(path)
        .returning(().pure[IO])

    def verifyMigrationExecutionRegistered =
      (executionRegister.registerExecution _)
        .expects(ProvisionProjectsGraph.name)
        .returning(().pure[IO])
  }
}
