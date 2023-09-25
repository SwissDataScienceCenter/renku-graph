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
import GraphModelGenerators.projectSlugs
import cats.MonadThrow
import io.renku.generators.Generators.{exceptions, nonEmptyStrings, positiveInts}
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues
import tooling._

class MigrationToV10Spec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory with EitherValues {

  "MigrationToV10" should {

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

    "prepare backlog of projects requiring migration " +
      "and start the process that " +
      "goes through all the projects, " +
      "wait until the env has capacity to serve a new re-provisioning event, " +
      "send the CLEAN_UP_REQUEST event to EL for each of them, " +
      "keep a note of the project once an event is sent for it" in new TestCase {

        givenBacklogCreated()

        val allProjects = projectSlugs.generateList(min = pageSize, max = pageSize * 2)

        val projectsPages = allProjects
          .sliding(pageSize, pageSize)
          .toList
        givenProjectsPagesReturned(projectsPages :+ List.empty[projects.Slug])

        givenProgressInfoFinding(returning = nonEmptyStrings().generateOne.pure[IO], times = allProjects.size)

        givenEnvHasCapabilitiesToTakeNextEvent

        allProjects map toCleanUpRequestEvent foreach verifyEventWasSent

        allProjects foreach verifyProjectNotedDone

        migration.migrate().value.unsafeRunSync().value shouldBe ()
      }

    "return a Recoverable Error if in case of an exception while finding projects " +
      "the given strategy returns one" in new TestCase {

        givenBacklogCreated()

        val exception = exceptions.generateOne
        (() => projectsFinder.nextProjectsPage())
          .expects()
          .returning(exception.raiseError[IO, List[projects.Slug]])

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

    private val eventCategoryName = CategoryName("CLEAN_UP_REQUEST")
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val migrationNeedChecker         = mock[MigrationNeedChecker[IO]]
    val backlogCreator               = mock[BacklogCreator[IO]]
    val projectsFinder               = mock[ProjectsPageFinder[IO]]
    private val progressFinder       = mock[ProgressFinder[IO]]
    private val envReadinessChecker  = mock[EnvReadinessChecker[IO]]
    private val eventSender          = mock[EventSender[IO]]
    private val projectDonePersister = mock[ProjectDonePersister[IO]]
    private val executionRegister    = mock[MigrationExecutionRegister[IO]]
    val recoverableError             = processingRecoverableErrors.generateOne
    private val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new MigrationToV10[IO](
      migrationNeedChecker,
      backlogCreator,
      projectsFinder,
      progressFinder,
      envReadinessChecker,
      eventSender,
      projectDonePersister,
      executionRegister,
      recoveryStrategy
    )

    def givenBacklogCreated() =
      (backlogCreator.createBacklog _)
        .expects()
        .returning(().pure[IO])

    def givenProjectsPagesReturned(pages: List[List[projects.Slug]]): Unit =
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

    def toCleanUpRequestEvent(projectSlug: projects.Slug) = projectSlug -> EventRequestContent.NoPayload {
      json"""{
        "categoryName": $eventCategoryName,
        "project": {
          "slug": $projectSlug
        }
      }"""
    }

    lazy val verifyEventWasSent: ((projects.Slug, EventRequestContent.NoPayload)) => Unit = { case (slug, event) =>
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(event,
                 EventSender.EventContext(eventCategoryName,
                                          show"$categoryName: ${migration.name} cannot send event for $slug"
                 )
        )
        .returning(().pure[IO])
      ()
    }

    def verifyProjectNotedDone(slug: projects.Slug) =
      (projectDonePersister.noteDone _)
        .expects(slug)
        .returning(().pure[IO])

    def verifyMigrationExecutionRegistered =
      (executionRegister.registerExecution _)
        .expects(MigrationToV10.name)
        .returning(().pure[IO])
  }
}
