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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.MonadThrow
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.eventlog.api.EventLogClient
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestampsNotInTheFuture}
import io.renku.graph.model.EventContentGenerators.eventInfos
import io.renku.graph.model.events
import io.renku.graph.model.events.{EventDate, EventInfo}
import io.renku.graph.model.projects.DateViewed
import io.renku.graph.model.testentities._
import io.renku.http.rest.paging.model.PerPage
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{EitherValues, Succeeded}
import tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery}

class ProjectsDateViewedCreatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with should.Matchers
    with EitherValues
    with AsyncMockFactory {

  "migrate" should {

    "issue PROJECT_VIEWED events for all project found in the TS " +
      "with the date taken from latest event Date" in projectsDSConfig.use { implicit pcc =>
        val projects = anyProjectEntities.generateNonEmptyList().toList

        for {
          _ <- uploadToProjects(projects: _*)

          eventDates = projects map { project =>
                         val eventDate =
                           timestampsNotInTheFuture(butYoungerThan = project.dateCreated.value).generateAs(EventDate)
                         givenSuccessfulFindingLatestEventInfo(project, returning = eventDate.some)
                         eventDate
                       }

          events = projects.zip(eventDates) map { case (project, eventDate) =>
                     ProjectViewedEvent(project.slug, DateViewed(eventDate.value), maybeUserId = None)
                   }

          _ = events foreach (givenProjectViewedEventSent(_, returning = ().pure[IO]))

          _ <- creator.migrate().value.asserting(_.value shouldBe ())
        } yield Succeeded
      }

    "issue PROJECT_VIEWED events for all project found in the TS " +
      "with the date taken from project Date Created" in projectsDSConfig.use { implicit pcc =>
        val projects = anyProjectEntities.generateNonEmptyList().toList

        for {
          _ <- uploadToProjects(projects: _*)

          _ = projects foreach (givenSuccessfulFindingLatestEventInfo(_, returning = Option.empty[EventDate]))

          events = projects map { project =>
                     ProjectViewedEvent(project.slug, DateViewed(project.dateCreated.value), maybeUserId = None)
                   }

          _ = events foreach (givenProjectViewedEventSent(_, returning = ().pure[IO]))

          _ <- creator.migrate().value.asserting(_.value shouldBe ())
        } yield Succeeded
      }

    "return a Recoverable Error if in case of an exception while finding projects " +
      "the given strategy returns one" in projectsDSConfig.use { implicit pcc =>
        val project = anyProjectEntities.generateOne

        for {
          _ <- uploadToProjects(project)

          exception = exceptions.generateOne
          _ = givenFindingLatestEventInfo(project, returning = exception.raiseError[IO, EventLogClient.Result.Failure])

          _ <- creator.migrate().value.asserting(_.left.value shouldBe recoverableError)
        } yield Succeeded
      }
  }

  private implicit lazy val ioLogger: TestLogger[IO] = TestLogger[IO]()
  private val elClient         = mock[EventLogClient[IO]]
  private val tgClient         = mock[triplesgenerator.api.events.Client[IO]]
  private val recoverableError = processingRecoverableErrors.generateOne
  private val recoveryStrategy = new RecoverableErrorsRecovery {
    override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
      recoverableError.asLeft[OUT].pure[F]
    }
  }
  private def creator(implicit pcc: ProjectsConnectionConfig) = {
    val executionRegister = mock[MigrationExecutionRegister[IO]]
    new ProjectsDateViewedCreator[IO](tsClient, elClient, tgClient, executionRegister, recoveryStrategy)
  }

  private def givenSuccessfulFindingLatestEventInfo(project: Project, returning: Option[events.EventDate]) = {

    val maybeEvent = returning.map(date => eventInfos().generateOne.copy(eventDate = date))

    givenFindingLatestEventInfo(project, returning = EventLogClient.Result.Success(maybeEvent.toList).pure[IO])
  }

  private def givenFindingLatestEventInfo(project: Project, returning: IO[EventLogClient.Result[List[EventInfo]]]) =
    (elClient.getEvents _)
      .expects(
        EventLogClient.SearchCriteria
          .forProject(project.slug)
          .sortBy(EventLogClient.SearchCriteria.Sort.EventDateDesc)
          .withPerPage(PerPage(1))
      )
      .returning(returning)

  private def givenProjectViewedEventSent(event: ProjectViewedEvent, returning: IO[Unit]) =
    (tgClient
      .send(_: ProjectViewedEvent))
      .expects(event)
      .returning(returning)
}
