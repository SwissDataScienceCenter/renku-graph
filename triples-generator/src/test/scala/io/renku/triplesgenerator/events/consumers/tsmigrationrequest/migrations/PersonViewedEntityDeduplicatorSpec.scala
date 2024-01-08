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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.entities.viewings.TestEntityViewings
import io.renku.entities.viewings.collector.persons.Generators.{generateProjectWithCreator, generateProjectWithCreatorAndDataset}
import io.renku.entities.viewings.collector.persons.{PersonViewedDatasetSpecTools, PersonViewedProjectSpecTools}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, projects}
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesgenerator.api.events.{DatasetViewedEvent, ProjectViewedEvent, UserId}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}

class PersonViewedProjectDeduplicatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with TestSearchInfoDatasets
    with PersonViewedProjectSpecTools
    with TestEntityViewings
    with should.Matchers
    with OptionValues {

  "run" should {

    "remove obsolete project viewing dates when multiple exist for a single user and project" in projectsDSConfig.use {
      implicit pcc =>
        // project1
        val project1UserId = UserId(personGitLabIds.generateOne)
        val project1       = generateProjectWithCreator(project1UserId)
        for {
          _ <- provisionProject(project1)

          eventProject1 = ProjectViewedEvent(
                            project1.slug,
                            projectViewedDates(project1.dateCreated.value).generateOne,
                            project1UserId.some
                          )
          _ <- provision(eventProject1)

          olderDateDataset1 = timestamps(max = eventProject1.dateViewed.value).generateAs(projects.DateViewed)
          _ <- insertOtherDate(project1.resourceId, olderDateDataset1)

          // project2
          project2UserId = UserId(personGitLabIds.generateOne)
          project2       = generateProjectWithCreator(project2UserId)
          _ <- provisionProject(project2)

          eventProject2 = ProjectViewedEvent(
                            project2.slug,
                            projectViewedDates(project2.dateCreated.value).generateOne,
                            project2UserId.some
                          )
          _ <- provision(eventProject2)

          // assumptions check
          project1UserResourceId = project1.maybeCreator.value.resourceId
          project2UserResourceId = project2.maybeCreator.value.resourceId

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project1UserResourceId, project1.resourceId, eventProject1.dateViewed),
                   ViewingRecord(project1UserResourceId, project1.resourceId, olderDateDataset1),
                   ViewingRecord(project2UserResourceId, project2.resourceId, eventProject2.dateViewed)
                 )
               }

          _ <- runUpdate(PersonViewedEntityDeduplicator.query)

          // verification
          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project1UserResourceId, project1.resourceId, eventProject1.dateViewed),
                   ViewingRecord(project2UserResourceId, project2.resourceId, eventProject2.dateViewed)
                 )
               }
        } yield Succeeded
    }
  }

  implicit lazy val ioLogger: TestLogger[IO] = TestLogger()
}

class PersonViewedDatasetDeduplicatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with TestSearchInfoDatasets
    with PersonViewedDatasetSpecTools
    with TestEntityViewings
    with should.Matchers
    with OptionValues {

  "run" should {

    "remove obsolete dataset viewing dates when multiple exist for a single user and dataset on a project" in projectsDSConfig
      .use { implicit pcc =>
        // project1
        val project1UserId              = personGitLabIds.generateOne
        val datasetProject1 -> project1 = generateProjectWithCreatorAndDataset(UserId(project1UserId))
        for {
          _ <- provisionProject(project1)

          eventDatasetProject1 = DatasetViewedEvent(
                                   datasetProject1.identification.identifier,
                                   datasetViewedDates(datasetProject1.provenance.date.instant).generateOne,
                                   project1UserId.some
                                 )
          _ <- provision(eventDatasetProject1)

          olderDateDataset1 = timestamps(max = eventDatasetProject1.dateViewed.value).generateAs(datasets.DateViewed)
          _ <- insertOtherDate(datasetProject1.resourceId, olderDateDataset1)

          // project2
          project2UserId              = personGitLabIds.generateOne
          datasetProject2 -> project2 = generateProjectWithCreatorAndDataset(UserId(project2UserId))
          _ <- provisionProject(project2)

          eventDatasetProject2 = DatasetViewedEvent(
                                   datasetProject2.identification.identifier,
                                   datasetViewedDates(datasetProject2.provenance.date.instant).generateOne,
                                   project2UserId.some
                                 )
          _ <- provision(eventDatasetProject2)

          // assumptions check
          project1UserResourceId = project1.maybeCreator.value.resourceId
          project2UserResourceId = project2.maybeCreator.value.resourceId

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project1UserResourceId, datasetProject1.resourceId, eventDatasetProject1.dateViewed),
                   ViewingRecord(project1UserResourceId, datasetProject1.resourceId, olderDateDataset1),
                   ViewingRecord(project2UserResourceId, datasetProject2.resourceId, eventDatasetProject2.dateViewed)
                 )
               }

          _ <- runUpdate(PersonViewedEntityDeduplicator.query)

          // verification
          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   ViewingRecord(project1UserResourceId, datasetProject1.resourceId, eventDatasetProject1.dateViewed),
                   ViewingRecord(project2UserResourceId, datasetProject2.resourceId, eventDatasetProject2.dateViewed)
                 )
               }
        } yield Succeeded
      }
  }

  implicit lazy val ioLogger: TestLogger[IO] = TestLogger()
}
