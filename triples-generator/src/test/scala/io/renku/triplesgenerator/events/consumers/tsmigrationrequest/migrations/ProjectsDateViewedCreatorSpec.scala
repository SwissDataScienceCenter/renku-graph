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
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import io.renku.triplesstore._
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery}

class ProjectsDateViewedCreatorSpec
    extends AnyWordSpec
    with should.Matchers
    with EitherValues
    with IOSpec
    with MockFactory
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "migrate" should {

    "issue PROJECT_VIEWED events for all project found in the TS " +
      "with the date taken from latest event Date" in new TestCase {

        val projects = anyProjectEntities.generateNonEmptyList().toList

        upload(to = projectsDataset, projects: _*)

        val eventDates = projects map { project =>
          val eventDate = timestampsNotInTheFuture(butYoungerThan = project.dateCreated.value).generateAs(EventDate)
          givenSuccessfulFindingLatestEventInfo(project, returning = eventDate.some)
          eventDate
        }

        val events = projects.zip(eventDates) map { case (project, eventDate) =>
          ProjectViewedEvent(project.slug, DateViewed(eventDate.value), maybeUserId = None)
        }

        events foreach (givenProjectViewedEventSent(_, returning = ().pure[IO]))

        creator.migrate().value.unsafeRunSync().value shouldBe ()
      }

    "issue PROJECT_VIEWED events for all project found in the TS " +
      "with the date taken from project Date Created" in new TestCase {

        val projects = anyProjectEntities.generateNonEmptyList().toList

        upload(to = projectsDataset, projects: _*)

        projects foreach (givenSuccessfulFindingLatestEventInfo(_, returning = Option.empty[EventDate]))

        val events = projects map { project =>
          ProjectViewedEvent(project.slug, DateViewed(project.dateCreated.value), maybeUserId = None)
        }

        events foreach (givenProjectViewedEventSent(_, returning = ().pure[IO]))

        creator.migrate().value.unsafeRunSync().value shouldBe ()
      }

    "return a Recoverable Error if in case of an exception while finding projects " +
      "the given strategy returns one" in new TestCase {

        val project = anyProjectEntities.generateOne

        upload(to = projectsDataset, project)

        val exception = exceptions.generateOne
        givenFindingLatestEventInfo(project, returning = exception.raiseError[IO, EventLogClient.Result.Failure])

        creator.migrate().value.unsafeRunSync().left.value shouldBe recoverableError
      }
  }

  private trait TestCase {

    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val tr:     SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private lazy val tsClient     = TSClient[IO](projectsDSConnectionInfo)
    private val elClient          = mock[EventLogClient[IO]]
    private val tgClient          = mock[triplesgenerator.api.events.Client[IO]]
    private val executionRegister = mock[MigrationExecutionRegister[IO]]
    val recoverableError          = processingRecoverableErrors.generateOne
    private val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    lazy val creator =
      new ProjectsDateViewedCreator[IO](tsClient, elClient, tgClient, executionRegister, recoveryStrategy)

    def givenSuccessfulFindingLatestEventInfo(project: Project, returning: Option[events.EventDate]) = {

      val maybeEvent = returning.map(date => eventInfos().generateOne.copy(eventDate = date))

      givenFindingLatestEventInfo(project, returning = EventLogClient.Result.Success(maybeEvent.toList).pure[IO])
    }

    def givenFindingLatestEventInfo(project: Project, returning: IO[EventLogClient.Result[List[EventInfo]]]) =
      (elClient.getEvents _)
        .expects(
          EventLogClient.SearchCriteria
            .forProject(project.slug)
            .sortBy(EventLogClient.SearchCriteria.Sort.EventDateDesc)
            .withPerPage(PerPage(1))
        )
        .returning(returning)

    def givenProjectViewedEventSent(event: ProjectViewedEvent, returning: IO[Unit]) =
      (tgClient
        .send(_: ProjectViewedEvent))
        .expects(event)
        .returning(returning)
  }
}
