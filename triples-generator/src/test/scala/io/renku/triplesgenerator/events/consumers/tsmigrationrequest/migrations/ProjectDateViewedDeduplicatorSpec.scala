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

import io.renku.entities.viewings.EntityViewings
import io.renku.entities.viewings.collector.projects.EventPersisterSpecTools
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators.projectViewedEvents
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectDateViewedDeduplicatorSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with EventPersisterSpecTools
    with ProjectsDataset
    with EntityViewings {

  "run" should {

    "remove obsolete project viewing dates when multiple exist for a single project" in {

      val project1 = anyProjectEntities.generateOne.to[entities.Project]
      upload(to = projectsDataset, project1)

      val eventProject1 = projectViewedEvents.generateOne.copy(slug = project1.slug)
      provision(eventProject1).unsafeRunSync()

      val otherProject1Date = timestamps(max = eventProject1.dateViewed.value).generateAs(projects.DateViewed)
      insertOtherDate(project1, otherProject1Date)

      val project2 = anyProjectEntities.generateOne.to[entities.Project]
      upload(to = projectsDataset, project2)

      val eventProject2 = projectViewedEvents.generateOne.copy(slug = project2.slug)
      provision(eventProject2).unsafeRunSync()

      findAllViewings shouldBe Set(
        project1.resourceId -> eventProject1.dateViewed,
        project1.resourceId -> otherProject1Date,
        project2.resourceId -> eventProject2.dateViewed
      )

      runUpdate(projectsDataset, ProjectDateViewedDeduplicator.query).unsafeRunSync()

      findAllViewings shouldBe Set(
        project1.resourceId -> eventProject1.dateViewed,
        project2.resourceId -> eventProject2.dateViewed
      )
    }
  }
}
