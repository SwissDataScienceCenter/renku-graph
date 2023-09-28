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
package lucenereindex

import cats.effect.IO
import cats.syntax.all._
import io.renku.eventlog
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.projectSlugs
import cats.MonadThrow
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.generators.Generators.{exceptions, nonEmptyStrings, positiveInts}
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import tooling._

class ReindexLuceneSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with EitherValues {

  it should "prepare backlog of all the projects in the TS " +
    "and start the process that " +
    "goes through all the projects, " +
    "wait until the env has capacity to serve a new re-provisioning event, " +
    "send the RedoProjectTransformation event to EL for each of them, " +
    "keep a note of the project once an event is sent for it" in {

      givenBacklogCreated()

      val allProjects = projectSlugs.generateList(min = pageSize, max = pageSize * 2)

      val projectsPages = allProjects
        .sliding(pageSize, pageSize)
        .toList
      givenProjectsPagesReturned(projectsPages :+ List.empty[projects.Slug])

      givenProgressInfoFinding(returning = nonEmptyStrings().generateOne.pure[IO], times = allProjects.size)

      givenEnvHasCapabilitiesToTakeNextEvent

      allProjects map givenRedoTransformationEventSent

      allProjects foreach verifyProjectNotedDone

      migration.migrate().value.asserting(_.value shouldBe ())
    }

  it should "return a Recoverable Error if in case of an exception while finding projects " +
    "the given strategy returns one" in {

      givenBacklogCreated()

      val exception = exceptions.generateOne
      (() => projectsFinder.nextProjectsPage)
        .expects()
        .returning(exception.raiseError[IO, List[projects.Slug]])

      migration.migrate().value.unsafeRunSync().left.value shouldBe recoverableError
    }

  private lazy val pageSize = positiveInts(max = 100).generateOne.value

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private val backlogCreator        = mock[BacklogCreator[IO]]
  private val projectsFinder        = mock[ProjectsPageFinder[IO]]
  private val progressFinder        = mock[ProgressFinder[IO]]
  private val envReadinessChecker   = mock[EnvReadinessChecker[IO]]
  private val elClient              = mock[eventlog.api.events.Client[IO]]
  private val projectDonePersister  = mock[ProjectDonePersister[IO]]
  private val executionRegister     = mock[MigrationExecutionRegister[IO]]
  private lazy val recoverableError = processingRecoverableErrors.generateOne
  private val recoveryStrategy = new RecoverableErrorsRecovery {
    override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
      recoverableError.asLeft[OUT].pure[F]
    }
  }
  private lazy val migration = new ReindexLucene[IO](
    backlogCreator,
    projectsFinder,
    progressFinder,
    envReadinessChecker,
    elClient,
    projectDonePersister,
    executionRegister,
    recoveryStrategy
  )

  private def givenBacklogCreated() =
    (backlogCreator.createBacklog _)
      .expects()
      .returning(().pure[IO])

  private def givenProjectsPagesReturned(pages: List[List[projects.Slug]]): Unit =
    pages foreach { page =>
      (() => projectsFinder.nextProjectsPage)
        .expects()
        .returning(page.pure[IO])
    }

  private def givenProgressInfoFinding(returning: IO[String], times: Int) =
    (() => progressFinder.findProgressInfo)
      .expects()
      .returning(returning)
      .repeat(times)

  private def givenEnvHasCapabilitiesToTakeNextEvent =
    (() => envReadinessChecker.envReadyToTakeEvent)
      .expects()
      .returning(().pure[IO])
      .atLeastOnce()

  private def givenRedoTransformationEventSent(slug: projects.Slug) =
    (elClient
      .send(_: StatusChangeEvent.RedoProjectTransformation))
      .expects(StatusChangeEvent.RedoProjectTransformation(slug))
      .returning(().pure[IO])

  private def verifyProjectNotedDone(slug: projects.Slug) =
    (projectDonePersister.noteDone _)
      .expects(slug)
      .returning(().pure[IO])
}
