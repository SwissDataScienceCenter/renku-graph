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
import io.renku.entities.viewings.collector.projects.EventPersisterSpecTools
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.api.events.Generators.projectViewedEvents
import io.renku.triplesstore.GraphJenaSpec
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class ProjectDateViewedDeduplicatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with EventPersisterSpecTools
    with TestEntityViewings
    with should.Matchers {

  "run" should {

    "remove obsolete project viewing dates when multiple exist for a single project" in projectsDSConfig.use {
      implicit pcc =>
        for {
          project1 <- anyProjectEntities.generateOne.to[entities.Project].pure[IO]
          _        <- provisionProject(project1)

          eventProject1 = projectViewedEvents.generateOne.copy(slug = project1.slug)
          _ <- provision(eventProject1)

          otherProject1Date = timestamps(max = eventProject1.dateViewed.value).generateAs(projects.DateViewed)
          _ <- insertOtherDate(project1, otherProject1Date)

          project2 = anyProjectEntities.generateOne.to[entities.Project]
          _ <- provisionProject(project2)

          eventProject2 = projectViewedEvents.generateOne.copy(slug = project2.slug)
          _ <- provision(eventProject2)

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   project1.resourceId -> eventProject1.dateViewed,
                   project1.resourceId -> otherProject1Date,
                   project2.resourceId -> eventProject2.dateViewed
                 )
               }

          _ <- runUpdate(ProjectDateViewedDeduplicator.query)

          _ <- findAllViewings.asserting {
                 _ shouldBe Set(
                   project1.resourceId -> eventProject1.dateViewed,
                   project2.resourceId -> eventProject2.dateViewed
                 )
               }
        } yield Succeeded
    }
  }

  implicit lazy val ioLogger: TestLogger[IO] = TestLogger()
}
