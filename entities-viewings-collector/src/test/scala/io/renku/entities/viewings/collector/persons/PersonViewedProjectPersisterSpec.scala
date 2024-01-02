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

package io.renku.entities.viewings.collector.persons

import cats.effect.IO
import cats.syntax.all._
import io.renku.entities.viewings.collector
import io.renku.entities.viewings.collector.persons.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators.userIds
import io.renku.triplesgenerator.api.events.UserId
import io.renku.triplesstore._
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PersonViewedProjectPersisterSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with PersonViewedProjectSpecTools
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  "persist" should {

    "insert the given GLUserViewedProject to the TS and run the deduplication query " +
      "if it doesn't exist yet " +
      "- case with a user identified with GitLab id" in new TestCase {

        val userId  = UserId(personGitLabIds.generateOne)
        val project = generateProjectWithCreator(userId)

        upload(to = projectsDataset, project)

        val dateViewed = projectViewedDates(project.dateCreated.value).generateOne
        val event      = GLUserViewedProject(userId, toCollectorProject(project), dateViewed)

        givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
        )
      }

    "insert the given GLUserViewedProject to the TS and run the deduplication query " +
      "if it doesn't exist yet " +
      "- case with a user identified with email" in new TestCase {

        val userId  = UserId(personEmails.generateOne)
        val project = generateProjectWithCreator(userId)

        upload(to = projectsDataset, project)

        val dateViewed = projectViewedDates(project.dateCreated.value).generateOne
        val event      = collector.persons.GLUserViewedProject(userId, toCollectorProject(project), dateViewed)

        givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
        )
      }

    "update the date for the user and project from the GLUserViewedProject and run the deduplicate query " +
      "if an event for the project already exists in the TS " +
      "and the date from the new event is newer than this in the TS" in new TestCase {

        val userId  = userIds.generateOne
        val project = generateProjectWithCreator(userId)

        upload(to = projectsDataset, project)

        val dateViewed = projectViewedDates(project.dateCreated.value).generateOne
        val event      = collector.persons.GLUserViewedProject(userId, toCollectorProject(project), dateViewed)

        givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
        )

        val newDate = timestampsNotInTheFuture(butYoungerThan = event.date.value).generateAs(projects.DateViewed)

        givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

        persister.persist(event.copy(date = newDate)).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, newDate))
      }

    "do nothing if the event date is older than the date in the TS" in new TestCase {

      val userId  = userIds.generateOne
      val project = generateProjectWithCreator(userId)

      upload(to = projectsDataset, project)

      val dateViewed = projectViewedDates(project.dateCreated.value).generateOne
      val event      = collector.persons.GLUserViewedProject(userId, toCollectorProject(project), dateViewed)

      givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

      persister.persist(event).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(
        ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
      )

      val newDate = timestamps(max = event.date.value.minusSeconds(1)).generateAs(projects.DateViewed)

      persister.persist(event.copy(date = newDate)).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed))
    }

    "update the date for the user and project from the GLUserViewedProject, run the deduplicate query " +
      "and leave other user viewings if they exist" in new TestCase {

        val userId   = userIds.generateOne
        val project1 = generateProjectWithCreator(userId)
        val project2 = generateProjectWithCreator(userId)

        upload(to = projectsDataset, project1, project2)

        val project1DateViewed = projectViewedDates(project1.dateCreated.value).generateOne
        val project1Event =
          collector.persons.GLUserViewedProject(userId, toCollectorProject(project1), project1DateViewed)
        givenEventDeduplication(project1.maybeCreator.value.resourceId, project1.resourceId, returning = ().pure[IO])
        persister.persist(project1Event).unsafeRunSync() shouldBe ()

        val project2DateViewed = projectViewedDates(project2.dateCreated.value).generateOne
        val project2Event =
          collector.persons.GLUserViewedProject(userId, toCollectorProject(project2), project2DateViewed)
        givenEventDeduplication(project2.maybeCreator.value.resourceId, project2.resourceId, returning = ().pure[IO])
        persister.persist(project2Event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project1.maybeCreator.value.resourceId, project1.resourceId, project1DateViewed),
          ViewingRecord(project2.maybeCreator.value.resourceId, project2.resourceId, project2DateViewed)
        )

        val newDate =
          timestampsNotInTheFuture(butYoungerThan = project1Event.date.value).generateAs(projects.DateViewed)

        givenEventDeduplication(project1.maybeCreator.value.resourceId, project1.resourceId, returning = ().pure[IO])
        persister.persist(project1Event.copy(date = newDate)).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project1.maybeCreator.value.resourceId, project1.resourceId, newDate),
          ViewingRecord(project2.maybeCreator.value.resourceId, project2.resourceId, project2DateViewed)
        )
      }

    "do nothing if the given event is for a non-existing user" in new TestCase {

      val project = generateProjectWithCreator(userIds.generateOne)

      val event = collector.persons.GLUserViewedProject(userIds.generateOne,
                                                        toCollectorProject(project),
                                                        projectViewedDates(project.dateCreated.value).generateOne
      )

      persister.persist(event).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set.empty
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient          = TSClient[IO](projectsDSConnectionInfo)
    private val eventDeduplicator = mock[PersonViewedProjectDeduplicator[IO]]
    val persister = new PersonViewedProjectPersisterImpl[IO](tsClient, PersonFinder(tsClient), eventDeduplicator)

    def givenEventDeduplication(personResourceId:  persons.ResourceId,
                                projectResourceId: projects.ResourceId,
                                returning:         IO[Unit]
    ) = (eventDeduplicator.deduplicate _)
      .expects(personResourceId, projectResourceId)
      .returning(returning)
  }
}
