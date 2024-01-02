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
import cats.syntax.all._
import io.renku.entities.viewings.collector.persons.PersonViewedProjectPersister
import io.renku.entities.viewings.collector.projects.viewed.EventPersisterImpl
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesstore._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDeduplicatorSpec
    extends AnyWordSpec
    with should.Matchers
    with EventPersisterSpecTools
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  "deduplicate" should {

    "do nothing if there's only one date for the project" in new TestCase {

      val project = anyProjectEntities.generateOne.to[entities.Project]
      upload(to = projectsDataset, project)

      val event = projectViewedEvents.generateOne.copy(slug = project.slug)

      persister.persist(event).unsafeRunSync() shouldBe ()

      deduplicator.deduplicate(project.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(project.resourceId -> event.dateViewed)
    }

    "leave only the latest date if there are many" in new TestCase {

      val project = anyProjectEntities.generateOne.to[entities.Project]
      upload(to = projectsDataset, project)

      val event = projectViewedEvents.generateOne.copy(slug = project.slug)
      persister.persist(event).unsafeRunSync() shouldBe ()

      val olderDateViewed1 = timestamps(max = event.dateViewed.value).generateAs(projects.DateViewed)
      insertOtherDate(project, olderDateViewed1)
      val olderDateViewed2 = timestamps(max = event.dateViewed.value).generateAs(projects.DateViewed)
      insertOtherDate(project, olderDateViewed2)

      findAllViewings shouldBe Set(
        project.resourceId -> event.dateViewed,
        project.resourceId -> olderDateViewed1,
        project.resourceId -> olderDateViewed2
      )

      deduplicator.deduplicate(project.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(project.resourceId -> event.dateViewed)
    }

    "do not remove dates for other projects" in new TestCase {

      val project1 = anyProjectEntities.generateOne.to[entities.Project]
      upload(to = projectsDataset, project1)
      val project2 = anyProjectEntities.generateOne.to[entities.Project]
      upload(to = projectsDataset, project2)

      val event1 = projectViewedEvents.generateOne.copy(slug = project1.slug)
      persister.persist(event1).unsafeRunSync() shouldBe ()
      val event2 = projectViewedEvents.generateOne.copy(slug = project2.slug)
      persister.persist(event2).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(
        project1.resourceId -> event1.dateViewed,
        project2.resourceId -> event2.dateViewed
      )

      deduplicator.deduplicate(project1.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(
        project1.resourceId -> event1.dateViewed,
        project2.resourceId -> event2.dateViewed
      )
    }
  }

  private trait TestCase {

    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient = TSClient[IO](projectsDSConnectionInfo)
    val deduplicator     = new EventDeduplicatorImpl[IO](tsClient, categoryNames.generateOne)

    private val personViewingPersister = mock[PersonViewedProjectPersister[IO]]
    (personViewingPersister.persist _).expects(*).returning(().pure[IO]).anyNumberOfTimes()
    val persister = new EventPersisterImpl[IO](tsClient, deduplicator, personViewingPersister)
  }
}
