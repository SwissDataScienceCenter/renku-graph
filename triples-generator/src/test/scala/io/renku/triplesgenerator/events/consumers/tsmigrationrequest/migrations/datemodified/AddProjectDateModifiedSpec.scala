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
package datemodified

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.{projectCreatedDates, projectPaths, projectResourceIds}
import cats.MonadThrow
import io.renku.generators.Generators.{exceptions, nonEmptyStrings, positiveInts}
import io.renku.interpreters.TestLogger
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery}

class AddProjectDateModifiedSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with EitherValues {

  "AddProjectDateModified" should {

    "be the ConditionedMigration" in {
      migration.getClass.getSuperclass shouldBe classOf[ConditionedMigration[IO]]
    }
  }

  "required" should {

    Set(
      ConditionedMigration.MigrationRequired.No(nonEmptyStrings().generateOne),
      ConditionedMigration.MigrationRequired.Yes(nonEmptyStrings().generateOne)
    ) foreach { answer =>
      s"return $answer if there are no projects to migrate" in {

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
      "fetch the project data from a relevant project graph," +
      "pass the project to the ProjectGraphProvisioner " +
      "and keep a note of the project once an event for it is sent" in {

        givenBacklogCreated()

        val allProjectPaths = projectPaths.generateList(min = pageSize, max = pageSize * 2)

        val projectPathsPages = allProjectPaths
          .sliding(pageSize, pageSize)
          .toList
        givenProjectsPagesReturned(projectPathsPages :+ List.empty[projects.Path])

        givenProgressInfoFinding(returning = nonEmptyStrings().generateOne.pure[IO], times = allProjectPaths.size)

        allProjectPaths foreach { path =>
          val projectInfo = ProjectInfo(projectResourceIds.generateOne, path, projectCreatedDates().generateOne)
          givenProjectDataFetching(path, returning = projectInfo.some.pure[IO])
          givenDatePersisting(projectInfo, returning = ().pure[IO])
          verifyProjectNotedDone(path)
        }

        migration.migrate().value.asserting(_.value shouldBe ())
      }

    "skip projects which cannot be found in the TS" in {

      givenBacklogCreated()

      val projectPath1 = projectPaths.generateOne
      val projectPath2 = projectPaths.generateOne
      val projectPath3 = projectPaths.generateOne

      val projectPathsPage = List(projectPath1, projectPath2, projectPath3)

      givenProjectsPagesReturned(List(projectPathsPage) :+ List.empty[projects.Path])

      givenProgressInfoFinding(returning = nonEmptyStrings().generateOne.pure[IO], times = projectPathsPage.size)

      List(projectPath1, projectPath3) foreach { path =>
        val projectInfo = ProjectInfo(projectResourceIds.generateOne, path, projectCreatedDates().generateOne)
        givenProjectDataFetching(path, returning = projectInfo.some.pure[IO])
        givenDatePersisting(projectInfo, returning = ().pure[IO])
        verifyProjectNotedDone(path)
      }

      givenProjectDataFetching(projectPath2, returning = None.pure[IO])
      verifyProjectNotedDone(projectPath2)

      migration.migrate().value.unsafeRunSync().value shouldBe ()
    }

    "return a Recoverable Error in case the recovery strategy returns one while finding projects" in {

      givenBacklogCreated()

      val exception = exceptions.generateOne
      (() => projectsFinder.nextProjectsPage())
        .expects()
        .returning(exception.raiseError[IO, List[projects.Path]])

      migration.migrate().value.unsafeRunSync().left.value shouldBe recoverableError
    }
  }

  "postMigration" should {

    "register migration execution" in {

      verifyMigrationExecutionRegistered

      migration.postMigration().value.asserting(_.value shouldBe ())
    }
  }

  private lazy val pageSize = positiveInts(max = 100).generateOne.value

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private val migrationNeedChecker  = mock[MigrationNeedChecker[IO]]
  private val backlogCreator        = mock[BacklogCreator[IO]]
  private val projectsFinder        = mock[ProjectsPageFinder[IO]]
  private val progressFinder        = mock[ProgressFinder[IO]]
  private val projectFetcher        = mock[ProjectFetcher[IO]]
  private val datePersister         = mock[DatePersister[IO]]
  private val projectDonePersister  = mock[ProjectDonePersister[IO]]
  private val executionRegister     = mock[MigrationExecutionRegister[IO]]
  private lazy val recoverableError = processingRecoverableErrors.generateOne
  private val recoveryStrategy = new RecoverableErrorsRecovery {
    override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
      recoverableError.asLeft[OUT].pure[F]
    }
  }
  private lazy val migration = new AddProjectDateModified[IO](
    migrationNeedChecker,
    backlogCreator,
    projectsFinder,
    progressFinder,
    projectFetcher,
    datePersister,
    projectDonePersister,
    executionRegister,
    recoveryStrategy
  )

  private def givenBacklogCreated() =
    (backlogCreator.createBacklog _)
      .expects()
      .returning(().pure[IO])

  private def givenProjectsPagesReturned(pages: List[List[projects.Path]]): Unit =
    pages foreach { page =>
      (projectsFinder.nextProjectsPage _)
        .expects()
        .returning(page.pure[IO])
    }

  private def givenProgressInfoFinding(returning: IO[String], times: Int) =
    (() => progressFinder.findProgressInfo)
      .expects()
      .returning(returning)
      .repeat(times)

  private def givenProjectDataFetching(path: projects.Path, returning: IO[Option[ProjectInfo]]) =
    (projectFetcher.fetchProject _)
      .expects(path)
      .returning(returning)

  private def givenDatePersisting(projectInfo: ProjectInfo, returning: IO[Unit]) =
    (datePersister.persistDateModified _)
      .expects(projectInfo)
      .returning(returning)

  private def verifyProjectNotedDone(path: projects.Path) =
    (projectDonePersister.noteDone _)
      .expects(path)
      .returning(().pure[IO])

  private def verifyMigrationExecutionRegistered =
    (executionRegister.registerExecution _)
      .expects(AddProjectDateModified.name)
      .returning(().pure[IO])
}
