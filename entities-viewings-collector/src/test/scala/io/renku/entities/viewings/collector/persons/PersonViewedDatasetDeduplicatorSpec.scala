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

package io.renku.entities.viewings.collector.persons

import cats.effect.IO
import io.renku.entities.viewings.collector.persons.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.UserId
import io.renku.triplesstore._
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PersonViewedDatasetDeduplicatorSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with PersonViewedDatasetSpecTools
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  "deduplicate" should {

    "do nothing if there's only one date for the user and dataset" in new TestCase {

      val userId             = UserId(personGitLabIds.generateOne)
      val dataset -> project = generateProjectWithCreatorAndDataset(userId)
      upload(to = projectsDataset, project)

      val event = GLUserViewedDataset(userId,
                                      toCollectorDataset(dataset),
                                      datasetViewedDates(dataset.provenance.date.instant).generateOne
      )
      persister.persist(event).unsafeRunSync() shouldBe ()

      val userResourceId = project.maybeCreator.value.resourceId
      deduplicator.deduplicate(userResourceId, dataset.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(ViewingRecord(userResourceId, dataset.resourceId, event.date))
    }

    "leave only the latest date if there are many" in new TestCase {

      val userId             = UserId(personGitLabIds.generateOne)
      val dataset -> project = generateProjectWithCreatorAndDataset(userId)
      upload(to = projectsDataset, project)

      val event = GLUserViewedDataset(userId,
                                      toCollectorDataset(dataset),
                                      datasetViewedDates(dataset.provenance.date.instant).generateOne
      )
      persister.persist(event).unsafeRunSync() shouldBe ()

      val olderDateViewed1 = timestamps(max = event.date.value).generateAs(datasets.DateViewed)
      insertOtherDate(dataset.resourceId, olderDateViewed1)
      val olderDateViewed2 = timestamps(max = event.date.value).generateAs(datasets.DateViewed)
      insertOtherDate(dataset.resourceId, olderDateViewed2)

      val userResourceId = project.maybeCreator.value.resourceId

      findAllViewings shouldBe Set(
        ViewingRecord(userResourceId, dataset.resourceId, event.date),
        ViewingRecord(userResourceId, dataset.resourceId, olderDateViewed1),
        ViewingRecord(userResourceId, dataset.resourceId, olderDateViewed2)
      )

      deduplicator.deduplicate(userResourceId, dataset.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(ViewingRecord(userResourceId, dataset.resourceId, event.date))
    }

    "do not remove dates for other projects" in new TestCase {

      val userId = UserId(personGitLabIds.generateOne)

      val dataset1 -> project1 = generateProjectWithCreatorAndDataset(userId)
      upload(to = projectsDataset, project1)

      val dataset2 -> project2 = generateProjectWithCreatorAndDataset(userId)
      upload(to = projectsDataset, project2)

      val event1 = GLUserViewedDataset(userId,
                                       toCollectorDataset(dataset1),
                                       datasetViewedDates(dataset1.provenance.date.instant).generateOne
      )
      persister.persist(event1).unsafeRunSync() shouldBe ()

      val event2 = GLUserViewedDataset(userId,
                                       toCollectorDataset(dataset2),
                                       datasetViewedDates(dataset2.provenance.date.instant).generateOne
      )
      persister.persist(event2).unsafeRunSync() shouldBe ()

      val userResourceId = project1.maybeCreator.value.resourceId

      findAllViewings shouldBe Set(
        ViewingRecord(userResourceId, dataset1.resourceId, event1.date),
        ViewingRecord(userResourceId, dataset2.resourceId, event2.date)
      )

      deduplicator.deduplicate(userResourceId, dataset1.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(
        ViewingRecord(userResourceId, dataset1.resourceId, event1.date),
        ViewingRecord(userResourceId, dataset2.resourceId, event2.date)
      )
    }
  }

  private trait TestCase {

    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient = TSClient[IO](projectsDSConnectionInfo)
    val deduplicator     = new PersonViewedDatasetDeduplicatorImpl[IO](tsClient)

    val persister = PersonViewedDatasetPersister[IO](tsClient)
  }
}
