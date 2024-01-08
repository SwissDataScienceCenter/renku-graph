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

package io.renku.entities.viewings.collector.projects

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.entities.viewings.ViewingsCollectorJenaSpec
import io.renku.entities.viewings.collector.persons.PersonViewedProjectPersister
import io.renku.entities.viewings.collector.projects.viewed.EventPersisterImpl
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class EventDeduplicatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with ViewingsCollectorJenaSpec
    with TestSearchInfoDatasets
    with EventPersisterSpecTools
    with should.Matchers
    with AsyncMockFactory {

  "deduplicate" should {

    "do nothing if there's only one date for the project" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]

      for {
        _ <- provisionProject(project)

        event = projectViewedEvents.generateOne.copy(slug = project.slug)

        _ <- persister.persist(event).assertNoException

        _ <- deduplicator.deduplicate(project.resourceId).assertNoException

        _ <- findAllViewings.asserting(_ shouldBe Set(project.resourceId -> event.dateViewed))
      } yield Succeeded
    }

    "leave only the latest date if there are many" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]
      for {
        _ <- provisionProject(project)

        event = projectViewedEvents.generateOne.copy(slug = project.slug)
        _ <- persister.persist(event).assertNoException

        olderDateViewed1 = timestamps(max = event.dateViewed.value).generateAs(projects.DateViewed)
        _ <- insertOtherDate(project, olderDateViewed1)
        olderDateViewed2 = timestamps(max = event.dateViewed.value).generateAs(projects.DateViewed)
        _ <- insertOtherDate(project, olderDateViewed2)

        _ <- findAllViewings.asserting {
               _ shouldBe Set(
                 project.resourceId -> event.dateViewed,
                 project.resourceId -> olderDateViewed1,
                 project.resourceId -> olderDateViewed2
               )
             }

        _ <- deduplicator.deduplicate(project.resourceId).assertNoException

        _ <- findAllViewings.asserting(_ shouldBe Set(project.resourceId -> event.dateViewed))
      } yield Succeeded
    }

    "do not remove dates for other projects" in projectsDSConfig.use { implicit pcc =>
      val project1 = anyProjectEntities.generateOne.to[entities.Project]
      val project2 = anyProjectEntities.generateOne.to[entities.Project]
      for {
        _ <- provisionProjects(project1, project2)

        event1 = projectViewedEvents.generateOne.copy(slug = project1.slug)
        _ <- persister.persist(event1).assertNoException
        event2 = projectViewedEvents.generateOne.copy(slug = project2.slug)
        _ <- persister.persist(event2).assertNoException

        _ <- findAllViewings.asserting {
               _ shouldBe Set(
                 project1.resourceId -> event1.dateViewed,
                 project2.resourceId -> event2.dateViewed
               )
             }

        _ <- deduplicator.deduplicate(project1.resourceId).assertNoException

        _ <- findAllViewings.asserting {
               _ shouldBe Set(
                 project1.resourceId -> event1.dateViewed,
                 project2.resourceId -> event2.dateViewed
               )
             }
      } yield Succeeded
    }
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()

  private def deduplicator(implicit pcc: ProjectsConnectionConfig) =
    new EventDeduplicatorImpl[IO](tsClient, categoryNames.generateOne)

  private def persister(implicit pcc: ProjectsConnectionConfig) = {

    val personViewingPersister = mock[PersonViewedProjectPersister[IO]]
    (personViewingPersister.persist _).expects(*).returning(().pure[IO]).anyNumberOfTimes()

    new EventPersisterImpl[IO](tsClient, deduplicator, personViewingPersister)
  }
}
