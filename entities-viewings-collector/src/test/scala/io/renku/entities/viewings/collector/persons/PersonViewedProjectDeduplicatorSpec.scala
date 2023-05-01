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
import eu.timepit.refined.auto._
import io.renku.entities.viewings.collector.persons.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.UserId
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PersonViewedProjectDeduplicatorSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with PersonViewedProjectSpecTools
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  "deduplicate" should {

    "do nothing if there's only one date for the user and project" in new TestCase {

      val userId  = UserId(personGitLabIds.generateOne)
      val project = generateProjectWithCreator(userId)
      upload(to = projectsDataset, project)

      val dateViewed = projectViewedDates(project.dateCreated.value).generateOne
      val event      = GLUserViewedProject(userId, toCollectorProject(project), dateViewed)
      persister.persist(event).unsafeRunSync() shouldBe ()

      val userResourceId = project.maybeCreator.value.resourceId
      deduplicator.deduplicate(userResourceId, project.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(
        ViewingRecord(project.maybeCreator.value.resourceId, project.resourceId, dateViewed)
      )
    }

    "leave only the latest date if there are many" in new TestCase {

      val userId  = UserId(personGitLabIds.generateOne)
      val project = generateProjectWithCreator(userId)
      upload(to = projectsDataset, project)

      val event = GLUserViewedProject(userId,
                                      toCollectorProject(project),
                                      projectViewedDates(project.dateCreated.value).generateOne
      )
      persister.persist(event).unsafeRunSync() shouldBe ()

      val olderDateViewed1 = timestamps(max = event.date.value).generateAs(projects.DateViewed)
      insertOtherDate(project.resourceId, olderDateViewed1)
      val olderDateViewed2 = timestamps(max = event.date.value).generateAs(projects.DateViewed)
      insertOtherDate(project.resourceId, olderDateViewed2)

      val userResourceId = project.maybeCreator.value.resourceId

      findAllViewings shouldBe Set(
        ViewingRecord(userResourceId, project.resourceId, event.date),
        ViewingRecord(userResourceId, project.resourceId, olderDateViewed1),
        ViewingRecord(userResourceId, project.resourceId, olderDateViewed2)
      )

      deduplicator.deduplicate(userResourceId, project.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(ViewingRecord(userResourceId, project.resourceId, event.date))
    }

    "do not remove dates for other projects" in new TestCase {

      val userId = UserId(personGitLabIds.generateOne)

      val project1 = generateProjectWithCreator(userId)
      upload(to = projectsDataset, project1)

      val project2 = generateProjectWithCreator(userId)
      upload(to = projectsDataset, project2)

      val event1 = GLUserViewedProject(userId,
                                       toCollectorProject(project1),
                                       projectViewedDates(project1.dateCreated.value).generateOne
      )
      persister.persist(event1).unsafeRunSync() shouldBe ()

      val event2 = GLUserViewedProject(userId,
                                       toCollectorProject(project2),
                                       projectViewedDates(project2.dateCreated.value).generateOne
      )
      persister.persist(event2).unsafeRunSync() shouldBe ()

      val userResourceId = project1.maybeCreator.value.resourceId

      findAllViewings shouldBe Set(
        ViewingRecord(userResourceId, project1.resourceId, event1.date),
        ViewingRecord(userResourceId, project2.resourceId, event2.date)
      )

      deduplicator.deduplicate(userResourceId, project1.resourceId).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(
        ViewingRecord(userResourceId, project1.resourceId, event1.date),
        ViewingRecord(userResourceId, project2.resourceId, event2.date)
      )
    }
  }

  private trait TestCase {

    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient = TSClient[IO](projectsDSConnectionInfo)
    val deduplicator     = new PersonViewedProjectDeduplicatorImpl[IO](tsClient)

    val persister = PersonViewedProjectPersister[IO](tsClient)
  }

  private def insertOtherDate(projectId: projects.ResourceId, dateViewed: projects.DateViewed) = runUpdate(
    on = projectsDataset,
    SparqlQuery.of(
      "test add another user project dateViewed",
      Prefixes of renku -> "renku",
      sparql"""|INSERT {
               |  GRAPH ${GraphClass.PersonViewings.id} {
               |    ?viewingId renku:dateViewed ${dateViewed.asObject}
               |  }
               |}
               |WHERE {
               |  GRAPH ${GraphClass.PersonViewings.id} {
               |    ?viewingId renku:project ${projectId.asEntityId}
               |  }
               |}
               |""".stripMargin
    )
  ).unsafeRunSync()
}
