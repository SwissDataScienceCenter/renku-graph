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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.entities.viewings.collector.persons.Generators._
import io.renku.entities.viewings.{ViewingsCollectorJenaSpec, collector}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.api.events.Generators.userIds
import io.renku.triplesgenerator.api.events.UserId
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}

class PersonViewedProjectPersisterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with ViewingsCollectorJenaSpec
    with TestSearchInfoDatasets
    with PersonViewedProjectSpecTools
    with should.Matchers
    with OptionValues
    with AsyncMockFactory {

  "persist" should {

    "insert the given GLUserViewedProject to the TS and run the deduplication query " +
      "if it doesn't exist yet " +
      "- case with a user identified with GitLab id" in projectsDSConfig.use { implicit pcc =>
        val userId  = UserId(personGitLabIds.generateOne)
        val project = generateProjectWithCreator(userId)

        for {
          _ <- provisionProject(project)

          dateViewed = projectViewedDates(project.dateCreated.value).generateOne
          event      = GLUserViewedProject(userId, toCollectorProject(project), dateViewed)

          _ =
            givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
                 )
               }
        } yield Succeeded
      }

    "insert the given GLUserViewedProject to the TS and run the deduplication query " +
      "if it doesn't exist yet " +
      "- case with a user identified with email" in projectsDSConfig.use { implicit pcc =>
        val userId  = UserId(personEmails.generateOne)
        val project = generateProjectWithCreator(userId)

        for {
          _ <- provisionProject(project)

          dateViewed = projectViewedDates(project.dateCreated.value).generateOne
          event      = collector.persons.GLUserViewedProject(userId, toCollectorProject(project), dateViewed)

          _ =
            givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
                 )
               }
        } yield Succeeded
      }

    "update the date for the user and project from the GLUserViewedProject and run the deduplicate query " +
      "if an event for the project already exists in the TS " +
      "and the date from the new event is newer than this in the TS" in projectsDSConfig.use { implicit pcc =>
        val userId  = userIds.generateOne
        val project = generateProjectWithCreator(userId)

        for {
          _ <- provisionProject(project)

          dateViewed = projectViewedDates(project.dateCreated.value).generateOne
          event      = collector.persons.GLUserViewedProject(userId, toCollectorProject(project), dateViewed)

          _ =
            givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting(
                 _ shouldBe Set(
                   ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
                 )
               )

          newDate = timestampsNotInTheFuture(butYoungerThan = event.date.value).generateAs(projects.DateViewed)

          _ =
            givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

          _ <- persister.persist(event.copy(date = newDate)).assertNoException

          _ <- findAllViewings.asserting(
                 _ shouldBe Set(ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, newDate))
               )
        } yield Succeeded
      }

    "do nothing if the event date is older than the date in the TS" in projectsDSConfig.use { implicit pcc =>
      val userId  = userIds.generateOne
      val project = generateProjectWithCreator(userId)

      for {
        _ <- provisionProject(project)

        dateViewed = projectViewedDates(project.dateCreated.value).generateOne
        event      = collector.persons.GLUserViewedProject(userId, toCollectorProject(project), dateViewed)

        _ = givenEventDeduplication(project.maybeCreator.value.resourceId, project.resourceId, returning = ().pure[IO])

        _ <- persister.persist(event).assertNoException

        _ <- findAllViewings.asserting {
               _ shouldBe Set(
                 ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
               )
             }

        newDate = timestamps(max = event.date.value.minusSeconds(1)).generateAs(projects.DateViewed)

        _ <- persister.persist(event.copy(date = newDate)).assertNoException

        _ <- findAllViewings.asserting {
               _ shouldBe Set(ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed))
             }
      } yield Succeeded
    }

    "update the date for the user and project from the GLUserViewedProject, run the deduplicate query " +
      "and leave other user viewings if they exist" in projectsDSConfig.use { implicit pcc =>
        val userId   = userIds.generateOne
        val project1 = generateProjectWithCreator(userId)
        val project2 = generateProjectWithCreator(userId)

        for {
          _ <- provisionProjects(project1, project2)

          project1DateViewed = projectViewedDates(project1.dateCreated.value).generateOne
          project1Event =
            collector.persons.GLUserViewedProject(userId, toCollectorProject(project1), project1DateViewed)
          _ = givenEventDeduplication(project1.maybeCreator.value.resourceId,
                                      project1.resourceId,
                                      returning = ().pure[IO]
              )
          _ <- persister.persist(project1Event).assertNoException

          project2DateViewed = projectViewedDates(project2.dateCreated.value).generateOne
          project2Event =
            collector.persons.GLUserViewedProject(userId, toCollectorProject(project2), project2DateViewed)
          _ = givenEventDeduplication(project2.maybeCreator.value.resourceId,
                                      project2.resourceId,
                                      returning = ().pure[IO]
              )
          _ <- persister.persist(project2Event).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project1.maybeCreator.value.resourceId, project1.resourceId, project1DateViewed),
                   ViewingRecord(project2.maybeCreator.value.resourceId, project2.resourceId, project2DateViewed)
                 )
               }

          newDate =
            timestampsNotInTheFuture(butYoungerThan = project1Event.date.value).generateAs(projects.DateViewed)

          _ = givenEventDeduplication(project1.maybeCreator.value.resourceId,
                                      project1.resourceId,
                                      returning = ().pure[IO]
              )
          _ <- persister.persist(project1Event.copy(date = newDate)).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project1.maybeCreator.value.resourceId, project1.resourceId, newDate),
                   ViewingRecord(project2.maybeCreator.value.resourceId, project2.resourceId, project2DateViewed)
                 )
               }
        } yield Succeeded
      }

    "do nothing if the given event is for a non-existing user" in projectsDSConfig.use { implicit pcc =>
      val project = generateProjectWithCreator(userIds.generateOne)

      val event = collector.persons.GLUserViewedProject(userIds.generateOne,
                                                        toCollectorProject(project),
                                                        projectViewedDates(project.dateCreated.value).generateOne
      )

      persister.persist(event).assertNoException >>
        findAllViewings.asserting(_ shouldBe Set.empty)
    }
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()
  private val eventDeduplicator = mock[PersonViewedProjectDeduplicator[IO]]
  private def persister(implicit pcc: ProjectsConnectionConfig) =
    new PersonViewedProjectPersisterImpl[IO](tsClient, PersonFinder(tsClient), eventDeduplicator)

  private def givenEventDeduplication(personResourceId:  persons.ResourceId,
                                      projectResourceId: projects.ResourceId,
                                      returning:         IO[Unit]
  ) = (eventDeduplicator.deduplicate _)
    .expects(personResourceId, projectResourceId)
    .returning(returning)
}
