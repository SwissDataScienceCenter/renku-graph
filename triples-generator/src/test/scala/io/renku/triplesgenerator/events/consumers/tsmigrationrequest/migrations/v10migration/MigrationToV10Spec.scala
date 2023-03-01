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
package v10migration

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.producers.EventSender
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.projectPaths
import cats.MonadThrow
import io.renku.generators.Generators.{exceptions, nonEmptyStrings, positiveInts}
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class MigrationToV10Spec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "MigrationToV10" should {

    "be the RegisteredMigration" in new TestCase {
      migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[IO]]
    }
  }

  "migrate" should {

    "go through all projects found in the TS, " +
      "wait until the env has capacity to serve a new re-provisioning event, " +
      "send the CLEAN_UP_REQUEST event to EL for each of them, " +
      "keep a note of the project once an event is sent for it, " +
      "explicitly run the post migration procedure " +
      "and remove info about all migrated projects" in new TestCase {

        val allProjects = projectPaths.generateList(min = pageSize, max = pageSize * 2)

        val projectsPages = allProjects
          .sliding(pageSize, pageSize)
          .toList
        givenProjectsPagesReturned(projectsPages :+ List.empty[projects.Path])

        givenProgressInfoFinding(returning = nonEmptyStrings().generateOne.pure[IO], times = allProjects.size)

        givenEnvHasCapabilitiesToTakeNextEvent

        allProjects map toCleanUpRequestEvent foreach verifyEventWasSent

        allProjects foreach verifyProjectNotedDone

        verifyMigrationExecutionRegistered

        verifyPostMigrationCleanUpRun

        migration.migrate().value.unsafeRunSync() shouldBe ().asRight
      }

    "return a Recoverable Error if in case of an exception while finding projects " +
      "the given strategy returns one" in new TestCase {

        val exception = exceptions.generateOne
        (() => projectsFinder.nextProjectsPage())
          .expects()
          .returning(exception.raiseError[IO, List[projects.Path]])

        migration.migrate().value.unsafeRunSync() shouldBe recoverableError.asLeft
      }
  }

  private trait TestCase {
    val pageSize = positiveInts(max = 100).generateOne.value

    private val eventCategoryName = CategoryName("CLEAN_UP_REQUEST")
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectsFinder               = mock[PagedProjectsFinder[IO]]
    private val progressFinder       = mock[ProgressFinder[IO]]
    private val envReadinessChecker  = mock[EnvReadinessChecker[IO]]
    private val eventSender          = mock[EventSender[IO]]
    private val projectDonePersister = mock[ProjectDonePersister[IO]]
    private val executionRegister    = mock[MigrationExecutionRegister[IO]]
    private val projectsDoneCleanUp  = mock[ProjectsDoneCleanUp[IO]]
    val recoverableError             = processingRecoverableErrors.generateOne
    private val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new MigrationToV10[IO](projectsFinder,
                                           progressFinder,
                                           envReadinessChecker,
                                           eventSender,
                                           projectDonePersister,
                                           executionRegister,
                                           projectsDoneCleanUp,
                                           recoveryStrategy
    )

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

    def givenEnvHasCapabilitiesToTakeNextEvent =
      (() => envReadinessChecker.envReadyToTakeEvent)
        .expects()
        .returning(().pure[IO])
        .atLeastOnce()

    def toCleanUpRequestEvent(projectPath: projects.Path) = projectPath -> EventRequestContent.NoPayload {
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
        .expects(MigrationToV10.name)
        .returning(().pure[IO])

    def verifyPostMigrationCleanUpRun =
      (projectsDoneCleanUp.cleanUp _)
        .expects()
        .returning(().pure[IO])
  }
}