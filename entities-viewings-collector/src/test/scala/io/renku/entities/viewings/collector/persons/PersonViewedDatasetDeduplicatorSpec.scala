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
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.entities.viewings.ViewingsCollectorJenaSpec
import io.renku.entities.viewings.collector.persons.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.api.events.UserId
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}

class PersonViewedDatasetDeduplicatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with ViewingsCollectorJenaSpec
    with TestSearchInfoDatasets
    with PersonViewedDatasetSpecTools
    with should.Matchers
    with OptionValues
    with AsyncMockFactory {

  "deduplicate" should {

    "do nothing if there's only one date for the user and dataset" in projectsDSConfig.use { implicit pcc =>
      val userId             = UserId(personGitLabIds.generateOne)
      val dataset -> project = generateProjectWithCreatorAndDataset(userId)

      for {
        _ <- provisionProject(project)

        event = GLUserViewedDataset(userId,
                                    toCollectorDataset(dataset),
                                    datasetViewedDates(dataset.provenance.date.instant).generateOne
                )
        _ <- persister.persist(event).assertNoException

        userResourceId = project.maybeCreator.value.resourceId
        _ <- deduplicator.deduplicate(userResourceId, dataset.resourceId).assertNoException

        _ <- findAllViewings.asserting(_ shouldBe Set(ViewingRecord(userResourceId, dataset.resourceId, event.date)))
      } yield Succeeded
    }

    "leave only the latest date if there are many" in projectsDSConfig.use { implicit pcc =>
      val userId             = UserId(personGitLabIds.generateOne)
      val dataset -> project = generateProjectWithCreatorAndDataset(userId)

      for {
        _ <- provisionProject(project)

        event = GLUserViewedDataset(userId,
                                    toCollectorDataset(dataset),
                                    datasetViewedDates(dataset.provenance.date.instant).generateOne
                )
        _ <- persister.persist(event).assertNoException

        olderDateViewed1 = timestamps(max = event.date.value).generateAs(datasets.DateViewed)
        _ <- insertOtherDate(dataset.resourceId, olderDateViewed1)
        olderDateViewed2 = timestamps(max = event.date.value).generateAs(datasets.DateViewed)
        _ <- insertOtherDate(dataset.resourceId, olderDateViewed2)

        userResourceId = project.maybeCreator.value.resourceId

        _ <- findAllViewings.asserting {
               _ shouldBe Set(
                 ViewingRecord(userResourceId, dataset.resourceId, event.date),
                 ViewingRecord(userResourceId, dataset.resourceId, olderDateViewed1),
                 ViewingRecord(userResourceId, dataset.resourceId, olderDateViewed2)
               )
             }

        _ <- deduplicator.deduplicate(userResourceId, dataset.resourceId).assertNoException

        _ <- findAllViewings.asserting(_ shouldBe Set(ViewingRecord(userResourceId, dataset.resourceId, event.date)))
      } yield Succeeded
    }

    "do not remove dates for other projects" in projectsDSConfig.use { implicit pcc =>
      val userId = UserId(personGitLabIds.generateOne)

      val dataset1 -> project1 = generateProjectWithCreatorAndDataset(userId)
      for {
        _ <- provisionProject(project1)

        dataset2 -> project2 = generateProjectWithCreatorAndDataset(userId)
        _ <- provisionProject(project2)

        event1 = GLUserViewedDataset(userId,
                                     toCollectorDataset(dataset1),
                                     datasetViewedDates(dataset1.provenance.date.instant).generateOne
                 )
        _ <- persister.persist(event1).assertNoException

        event2 = GLUserViewedDataset(userId,
                                     toCollectorDataset(dataset2),
                                     datasetViewedDates(dataset2.provenance.date.instant).generateOne
                 )
        _ <- persister.persist(event2).assertNoException

        userResourceId = project1.maybeCreator.value.resourceId

        _ <- findAllViewings.asserting {
               _ shouldBe Set(
                 ViewingRecord(userResourceId, dataset1.resourceId, event1.date),
                 ViewingRecord(userResourceId, dataset2.resourceId, event2.date)
               )
             }

        _ <- deduplicator.deduplicate(userResourceId, dataset1.resourceId).assertNoException

        _ <- findAllViewings.asserting {
               _ shouldBe Set(
                 ViewingRecord(userResourceId, dataset1.resourceId, event1.date),
                 ViewingRecord(userResourceId, dataset2.resourceId, event2.date)
               )
             }
      } yield Succeeded
    }
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()
  private def deduplicator(implicit pcc: ProjectsConnectionConfig) =
    new PersonViewedDatasetDeduplicatorImpl[IO](tsClient)

  private def persister(implicit pcc: ProjectsConnectionConfig) =
    PersonViewedDatasetPersister[IO](tsClient)
}
