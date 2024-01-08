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

package io.renku.entities.viewings.collector.projects
package viewed

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.entities.viewings.collector.persons.{GLUserViewedProject, PersonViewedProjectPersister}
import io.renku.entities.viewings.{ViewingsCollectorJenaSpec, collector}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.projects
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class EventPersisterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with ViewingsCollectorJenaSpec
    with TestSearchInfoDatasets
    with EventPersisterSpecTools
    with should.Matchers
    with AsyncMockFactory {

  "persist" should {

    "insert the given ProjectViewedEvent to the TS, " +
      "run the deduplication query and " +
      "persist a PersonViewing event " +
      "if there's no event for the project yet" in projectsDSConfig.use { implicit pcc =>
        val project = anyProjectEntities.generateOne
        for {
          _ <- provisionTestProject(project)

          event = projectViewedEvents.generateOne.copy(slug = project.slug)

          _ = givenEventDeduplication(project, returning = ().pure[IO])

          _ = givenPersonViewingPersisting(event, project, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting(_ shouldBe Set(project.resourceId -> event.dateViewed))
        } yield Succeeded
      }

    "update the date for the project from the ProjectViewedEvent, " +
      "run the deduplication query " +
      "if an event for the project already exists in the TS " +
      "and the date from the event is newer than this in the TS" in projectsDSConfig.use { implicit pcc =>
        val project = anyProjectEntities.generateOne
        for {
          _ <- provisionTestProject(project)

          event = projectViewedEvents.generateOne.copy(slug = project.slug)
          _     = givenEventDeduplication(project, returning = ().pure[IO])
          _     = givenPersonViewingPersisting(event, project, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting(_ shouldBe Set(project.resourceId -> event.dateViewed))

          newDate  = timestampsNotInTheFuture(butYoungerThan = event.dateViewed.value).generateAs(projects.DateViewed)
          newEvent = event.copy(dateViewed = newDate)

          _ = givenEventDeduplication(project, returning = ().pure[IO])
          _ = givenPersonViewingPersisting(newEvent, project, returning = ().pure[IO])

          _ <- persister.persist(newEvent).assertNoException

          _ <- findAllViewings.asserting(_ shouldBe Set(project.resourceId -> newDate))
        } yield Succeeded
      }

    "do nothing if the event date is older than the date in the TS" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne
      for {
        _ <- provisionTestProject(project)

        event = projectViewedEvents.generateOne.copy(slug = project.slug)

        _ = givenEventDeduplication(project, returning = ().pure[IO])
        _ = givenPersonViewingPersisting(event, project, returning = ().pure[IO])

        _ <- persister.persist(event).assertNoException

        _ <- findAllViewings.asserting(_ shouldBe Set(project.resourceId -> event.dateViewed))

        newDate  = timestamps(max = event.dateViewed.value.minusSeconds(1)).generateAs(projects.DateViewed)
        newEvent = event.copy(dateViewed = newDate)

        _ = givenPersonViewingPersisting(newEvent, project, returning = ().pure[IO])

        _ <- persister.persist(newEvent).assertNoException

        _ <- findAllViewings.asserting(_ shouldBe Set(project.resourceId -> event.dateViewed))
      } yield Succeeded
    }

    "do nothing if the given event is for a non-existing project" in projectsDSConfig.use { implicit pcc =>
      val event = projectViewedEvents.generateOne

      persister.persist(event).assertNoException >>
        findAllViewings.asserting(_ shouldBe Set.empty)
    }
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()
  private val eventDeduplicator      = mock[EventDeduplicator[IO]]
  private val personViewingPersister = mock[PersonViewedProjectPersister[IO]]
  private def persister(implicit pcc: ProjectsConnectionConfig) =
    new EventPersisterImpl[IO](tsClient, eventDeduplicator, personViewingPersister)

  private def givenEventDeduplication(project: Project, returning: IO[Unit]) =
    (eventDeduplicator.deduplicate _)
      .expects(project.resourceId)
      .returning(returning)

  private def givenPersonViewingPersisting(event: ProjectViewedEvent, project: Project, returning: IO[Unit]) =
    event.maybeUserId.map(userId =>
      (personViewingPersister.persist _)
        .expects(
          GLUserViewedProject(userId, collector.persons.Project(project.resourceId, project.slug), event.dateViewed)
        )
        .returning(returning)
    )
}
