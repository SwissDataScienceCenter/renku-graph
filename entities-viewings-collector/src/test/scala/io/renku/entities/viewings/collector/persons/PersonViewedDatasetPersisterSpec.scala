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
import io.renku.entities.viewings.collector
import io.renku.entities.viewings.collector.persons.Generators._
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

class PersonViewedDatasetPersisterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with PersonViewedDatasetSpecTools
    with should.Matchers
    with OptionValues
    with AsyncMockFactory {

  "persist" should {

    "insert the given GLUserViewedDataset to the TS and run the deduplicate query " +
      "if the viewing doesn't exist yet " +
      "- case with a user identified with GitLab id" in projectsDSConfig.use { implicit pcc =>
        val userId             = UserId(personGitLabIds.generateOne)
        val dataset -> project = generateProjectWithCreatorAndDataset(userId)

        for {
          _ <- provisionProject(project)

          dateViewed = datasetViewedDates(dataset.provenance.date.instant).generateOne
          event      = GLUserViewedDataset(userId, toCollectorDataset(dataset), dateViewed)

          _ =
            givenEventDeduplication(project.maybeCreator.value.resourceId, dataset.resourceId, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed)
                 )
               }
        } yield Succeeded
      }

    "insert the given GLUserViewedDataset to the TS and run the deduplicate query " +
      "if the viewing doesn't exist yet " +
      "- case with a user identified with email" in projectsDSConfig.use { implicit pcc =>
        val userId             = UserId(personEmails.generateOne)
        val dataset -> project = generateProjectWithCreatorAndDataset(userId)

        for {
          _ <- provisionProject(project)

          dateViewed = datasetViewedDates(dataset.provenance.date.instant).generateOne
          event      = GLUserViewedDataset(userId, toCollectorDataset(dataset), dateViewed)

          _ =
            givenEventDeduplication(project.maybeCreator.value.resourceId, dataset.resourceId, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed)
                 )
               }
        } yield Succeeded
      }

    "update the date for the user and ds from the GLUserViewedDataset and run the deduplicate query " +
      "if an event for the ds already exists in the TS " +
      "and the date from the new event is newer than this in the TS" in projectsDSConfig.use { implicit pcc =>
        val userId             = userIds.generateOne
        val dataset -> project = generateProjectWithCreatorAndDataset(userId)

        for {
          _ <- provisionProject(project)

          dateViewed = datasetViewedDates(dataset.provenance.date.instant).generateOne
          event      = GLUserViewedDataset(userId, toCollectorDataset(dataset), dateViewed)

          _ =
            givenEventDeduplication(project.maybeCreator.value.resourceId, dataset.resourceId, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed)
                 )
               }

          newDate = timestampsNotInTheFuture(butYoungerThan = event.date.value).generateAs(datasets.DateViewed)

          _ =
            givenEventDeduplication(project.maybeCreator.value.resourceId, dataset.resourceId, returning = ().pure[IO])

          _ <- persister.persist(event.copy(date = newDate)).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, newDate))
               }
        } yield Succeeded
      }

    "do nothing if the event date is older than the date in the TS" in projectsDSConfig.use { implicit pcc =>
      val userId             = userIds.generateOne
      val dataset -> project = generateProjectWithCreatorAndDataset(userId)

      for {
        _ <- provisionProject(project)

        dateViewed = datasetViewedDates(dataset.provenance.date.instant).generateOne
        event      = GLUserViewedDataset(userId, toCollectorDataset(dataset), dateViewed)

        _ = givenEventDeduplication(project.maybeCreator.value.resourceId, dataset.resourceId, returning = ().pure[IO])

        _ <- persister.persist(event).assertNoException

        _ <- findAllViewings.asserting(
               _ shouldBe Set(
                 ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed)
               )
             )

        newDate = timestamps(max = event.date.value.minusSeconds(1)).generateAs(datasets.DateViewed)

        _ <- persister.persist(event.copy(date = newDate)).assertNoException

        _ <- findAllViewings.asserting(
               _ shouldBe Set(ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed))
             )
      } yield Succeeded
    }

    "update the date for the user and project from the GLUserViewedProject, run the deduplicate query" +
      "and leave other user viewings if they exist" in projectsDSConfig.use { implicit pcc =>
        val userId               = userIds.generateOne
        val dataset1 -> project1 = generateProjectWithCreatorAndDataset(userId)
        val dataset2 -> project2 = generateProjectWithCreatorAndDataset(userId)

        for {
          _ <- provisionProjects(project1, project2)

          dataset1DateViewed = datasetViewedDates(dataset1.provenance.date.instant).generateOne
          dataset1Event =
            collector.persons.GLUserViewedDataset(userId, toCollectorDataset(dataset1), dataset1DateViewed)
          _ = givenEventDeduplication(project1.maybeCreator.value.resourceId,
                                      dataset1.resourceId,
                                      returning = ().pure[IO]
              )
          _ <- persister.persist(dataset1Event).assertNoException

          dataset2DateViewed = datasetViewedDates(dataset2.provenance.date.instant).generateOne
          dataset2Event =
            collector.persons.GLUserViewedDataset(userId, toCollectorDataset(dataset2), dataset2DateViewed)
          _ = givenEventDeduplication(project2.maybeCreator.value.resourceId,
                                      dataset2.resourceId,
                                      returning = ().pure[IO]
              )
          _ <- persister.persist(dataset2Event).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project1.maybeCreator.value.resourceId, dataset1.resourceId, dataset1DateViewed),
                   ViewingRecord(project2.maybeCreator.value.resourceId, dataset2.resourceId, dataset2DateViewed)
                 )
               }

          newDate =
            timestampsNotInTheFuture(butYoungerThan = dataset1Event.date.value).generateAs(datasets.DateViewed)

          _ = givenEventDeduplication(project1.maybeCreator.value.resourceId,
                                      dataset1.resourceId,
                                      returning = ().pure[IO]
              )
          _ <- persister.persist(dataset1Event.copy(date = newDate)).assertNoException

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project1.maybeCreator.value.resourceId, dataset1.resourceId, newDate),
                   ViewingRecord(project2.maybeCreator.value.resourceId, dataset2.resourceId, dataset2DateViewed)
                 )
               }
        } yield Succeeded
      }

    "do nothing if the given event is for a non-existing user" in projectsDSConfig.use { implicit pcc =>
      val dataset -> _ = generateProjectWithCreatorAndDataset(userIds.generateOne)

      val event = collector.persons.GLUserViewedDataset(userIds.generateOne,
                                                        toCollectorDataset(dataset),
                                                        datasetViewedDates(dataset.provenance.date.instant).generateOne
      )

      persister.persist(event).assertNoException >>
        findAllViewings.asserting(_ shouldBe Set.empty)
    }
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()
  private val eventDeduplicator = mock[PersonViewedDatasetDeduplicator[IO]]
  private def persister(implicit pcc: ProjectsConnectionConfig) =
    new PersonViewedDatasetPersisterImpl[IO](tsClient, PersonFinder(tsClient), eventDeduplicator)

  private def givenEventDeduplication(personResourceId:  persons.ResourceId,
                                      datasetResourceId: datasets.ResourceId,
                                      returning:         IO[Unit]
  ) = (eventDeduplicator.deduplicate _)
    .expects(personResourceId, datasetResourceId)
    .returning(returning)
}
