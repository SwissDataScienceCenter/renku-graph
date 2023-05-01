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

import io.renku.entities.viewings.collector.ProjectViewedTimeOntology
import io.renku.entities.viewings.collector.projects.EventPersisterSpecTools
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, projects}
import io.renku.jsonld.syntax._
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators.projectViewedEvents
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectDateViewedDeduplicatorSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with EventPersisterSpecTools
    with ProjectsDataset {

  "run" should {

    "remove obsolete project viewing dates when multiple exist for a single project" in {

      val event1Project1 = projectViewedEvents.generateOne
      insertEvent(event1Project1)

      val event2Project1 = projectViewedEvents.generateOne.copy(
        path = event1Project1.path,
        dateViewed = timestamps(max = event1Project1.dateViewed.value).generateAs(projects.DateViewed)
      )
      insertEvent(event2Project1)

      val eventProject2 = projectViewedEvents.generateOne
      insertEvent(eventProject2)

      findAllViewings shouldBe Set(
        projects.ResourceId(event1Project1.path) -> event1Project1.dateViewed,
        projects.ResourceId(event2Project1.path) -> event2Project1.dateViewed,
        projects.ResourceId(eventProject2.path)  -> eventProject2.dateViewed
      )

      runUpdate(projectsDataset, ProjectDateViewedDeduplicator.query).unsafeRunSync()

      findAllViewings shouldBe Set(
        projects.ResourceId(event1Project1.path) -> event1Project1.dateViewed,
        projects.ResourceId(eventProject2.path)  -> eventProject2.dateViewed
      )
    }
  }

  private def insertEvent(event: ProjectViewedEvent) =
    insert(
      to = projectsDataset,
      Quad(
        GraphClass.ProjectViewedTimes.id,
        projects.ResourceId(event.path).asEntityId,
        ProjectViewedTimeOntology.dataViewedProperty.id,
        event.dateViewed.asObject
      )
    )
}
