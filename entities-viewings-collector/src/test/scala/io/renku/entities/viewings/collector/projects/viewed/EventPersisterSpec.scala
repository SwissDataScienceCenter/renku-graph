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

package io.renku.entities.viewings.collector.projects
package viewed

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.viewings.collector
import io.renku.entities.viewings.collector.persons.{GLUserViewedProject, PersonViewedProjectPersister}
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{projects, GraphClass}
import io.renku.graph.model.testentities._
import io.renku.graph.model.Schemas.renku
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class EventPersisterSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  "persist" should {

    "insert the given ProjectViewedEvent to the TS and persist it in the PersonViewing " +
      "if there's no event for the project yet" in new TestCase {

        val project = anyProjectEntities.generateOne
        upload(to = projectsDataset, project)

        val event = projectViewedEvents.generateOne.copy(path = project.path)

        givenPersonViewingPersisting(event, project, returning = ().pure[IO])

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(project.resourceId -> event.dateViewed)
      }

    "update the date for the project from the ProjectViewedEvent " +
      "if an event for the project already exists in the TS " +
      "and the date from the event is newer than this in the TS" in new TestCase {

        val project = anyProjectEntities.generateOne
        upload(to = projectsDataset, project)

        val event = projectViewedEvents.generateOne.copy(path = project.path)

        givenPersonViewingPersisting(event, project, returning = ().pure[IO])

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(project.resourceId -> event.dateViewed)

        val newDate  = timestampsNotInTheFuture(butYoungerThan = event.dateViewed.value).generateAs(projects.DateViewed)
        val newEvent = event.copy(dateViewed = newDate)

        givenPersonViewingPersisting(newEvent, project, returning = ().pure[IO])

        persister.persist(newEvent).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(project.resourceId -> newDate)
      }

    "do nothing if the event date is older than the date in the TS" in new TestCase {

      val project = anyProjectEntities.generateOne
      upload(to = projectsDataset, project)

      val event = projectViewedEvents.generateOne.copy(path = project.path)

      givenPersonViewingPersisting(event, project, returning = ().pure[IO])

      persister.persist(event).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(project.resourceId -> event.dateViewed)

      val newDate  = timestamps(max = event.dateViewed.value.minusSeconds(1)).generateAs(projects.DateViewed)
      val newEvent = event.copy(dateViewed = newDate)

      givenPersonViewingPersisting(newEvent, project, returning = ().pure[IO])

      persister.persist(newEvent).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(project.resourceId -> event.dateViewed)
    }

    "do nothing if the given event is for a non-existing project" in new TestCase {

      val event = projectViewedEvents.generateOne

      persister.persist(event).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set.empty
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val personViewingPersister = mock[PersonViewedProjectPersister[IO]]
    val persister = new EventPersisterImpl[IO](TSClient[IO](projectsDSConnectionInfo), personViewingPersister)

    def givenPersonViewingPersisting(event: ProjectViewedEvent, project: Project, returning: IO[Unit]) =
      event.maybeUserId.map(userId =>
        (personViewingPersister.persist _)
          .expects(
            GLUserViewedProject(userId, collector.Project(project.resourceId, project.path), event.dateViewed)
          )
          .returning(returning)
      )
  }

  private def findAllViewings =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find project viewing",
        Prefixes of renku -> "renku",
        s"""|SELECT ?id ?date
            |FROM ${GraphClass.ProjectViewedTimes.id.asSparql.sparql} {
            |  ?id renku:dateViewed ?date.
            |}
            |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => projects.ResourceId(row("id")) -> projects.DateViewed(Instant.parse(row("date"))))
      .toSet
}
