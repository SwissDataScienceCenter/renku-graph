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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.syntax.all._
import io.renku.entities.viewings.EntityViewings
import io.renku.entities.viewings.collector.persons.Generators.{generateProjectWithCreator, generateProjectWithCreatorAndDataset}
import io.renku.entities.viewings.collector.persons.{PersonViewedDatasetSpecTools, PersonViewedProjectSpecTools}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, projects}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.{DatasetViewedEvent, ProjectViewedEvent, UserId}
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset}
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PersonViewedProjectDeduplicatorSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with IOSpec
    with InMemoryJenaForSpec
    with PersonViewedProjectSpecTools
    with ProjectsDataset
    with EntityViewings {

  "run" should {

    "remove obsolete project viewing dates when multiple exist for a single user and project" in {

      // project1
      val project1UserId = UserId(personGitLabIds.generateOne)
      val project1       = generateProjectWithCreator(project1UserId)
      upload(to = projectsDataset, project1)

      val eventProject1 = ProjectViewedEvent(
        project1.path,
        projectViewedDates(project1.dateCreated.value).generateOne,
        project1UserId.some
      )
      provision(eventProject1).unsafeRunSync()

      val olderDateDataset1 = timestamps(max = eventProject1.dateViewed.value).generateAs(projects.DateViewed)
      insertOtherDate(project1.resourceId, olderDateDataset1)

      // project2
      val project2UserId = UserId(personGitLabIds.generateOne)
      val project2       = generateProjectWithCreator(project2UserId)
      upload(to = projectsDataset, project2)

      val eventProject2 = ProjectViewedEvent(
        project2.path,
        projectViewedDates(project2.dateCreated.value).generateOne,
        project2UserId.some
      )
      provision(eventProject2).unsafeRunSync()

      // assumptions check
      val project1UserResourceId = project1.maybeCreator.value.resourceId
      val project2UserResourceId = project2.maybeCreator.value.resourceId

      findAllViewings shouldBe Set(
        ViewingRecord(project1UserResourceId, project1.resourceId, eventProject1.dateViewed),
        ViewingRecord(project1UserResourceId, project1.resourceId, olderDateDataset1),
        ViewingRecord(project2UserResourceId, project2.resourceId, eventProject2.dateViewed)
      )

      runUpdate(projectsDataset, PersonViewedEntityDeduplicator.query).unsafeRunSync()

      // verification
      findAllViewings shouldBe Set(
        ViewingRecord(project1UserResourceId, project1.resourceId, eventProject1.dateViewed),
        ViewingRecord(project2UserResourceId, project2.resourceId, eventProject2.dateViewed)
      )
    }
  }
}

class PersonViewedDatasetDeduplicatorSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with IOSpec
    with InMemoryJenaForSpec
    with PersonViewedDatasetSpecTools
    with ProjectsDataset
    with EntityViewings {

  "run" should {

    "remove obsolete dataset viewing dates when multiple exist for a single user and dataset on a project" in {

      // project1
      val project1UserId              = personGitLabIds.generateOne
      val datasetProject1 -> project1 = generateProjectWithCreatorAndDataset(UserId(project1UserId))
      upload(to = projectsDataset, project1)

      val eventDatasetProject1 = DatasetViewedEvent(
        datasetProject1.identification.identifier,
        datasetViewedDates(datasetProject1.provenance.date.instant).generateOne,
        project1UserId.some
      )
      provision(eventDatasetProject1).unsafeRunSync()

      val olderDateDataset1 = timestamps(max = eventDatasetProject1.dateViewed.value).generateAs(datasets.DateViewed)
      insertOtherDate(datasetProject1.resourceId, olderDateDataset1)

      // project2
      val project2UserId              = personGitLabIds.generateOne
      val datasetProject2 -> project2 = generateProjectWithCreatorAndDataset(UserId(project2UserId))
      upload(to = projectsDataset, project2)

      val eventDatasetProject2 = DatasetViewedEvent(
        datasetProject2.identification.identifier,
        datasetViewedDates(datasetProject2.provenance.date.instant).generateOne,
        project2UserId.some
      )
      provision(eventDatasetProject2).unsafeRunSync()

      // assumptions check
      val project1UserResourceId = project1.maybeCreator.value.resourceId
      val project2UserResourceId = project2.maybeCreator.value.resourceId

      findAllViewings shouldBe Set(
        ViewingRecord(project1UserResourceId, datasetProject1.resourceId, eventDatasetProject1.dateViewed),
        ViewingRecord(project1UserResourceId, datasetProject1.resourceId, olderDateDataset1),
        ViewingRecord(project2UserResourceId, datasetProject2.resourceId, eventDatasetProject2.dateViewed)
      )

      runUpdate(projectsDataset, PersonViewedEntityDeduplicator.query).unsafeRunSync()

      // verification
      findAllViewings shouldBe Set(
        ViewingRecord(project1UserResourceId, datasetProject1.resourceId, eventDatasetProject1.dateViewed),
        ViewingRecord(project2UserResourceId, datasetProject2.resourceId, eventDatasetProject2.dateViewed)
      )
    }
  }
}
