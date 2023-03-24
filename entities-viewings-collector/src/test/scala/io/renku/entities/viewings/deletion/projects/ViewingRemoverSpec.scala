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

package io.renku.entities.viewings
package deletion.projects

import cats.effect.IO
import cats.syntax.all._
import collector.projects.viewed.{EventPersisterImpl, PersonViewedProjectPersister}
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{persons, projects, GraphClass}
import io.renku.graph.model.testentities._
import io.renku.graph.model.Schemas.renku
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.ProjectViewingDeletion
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.OptionValues

class ViewingRemoverSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "removeViewing" should {

    "remove the relevant triples from the ProjectViewedTime and PersonViewing graphs" in new TestCase {

      val personGLId = personGitLabIds.generateOne
      val project    = generateProjectWithCreator(personGLId)
      insertViewing(project, personGLId)

      val otherProject = generateProjectWithCreator(personGLId)
      insertViewing(otherProject, personGLId)

      val personId = otherProject.maybeCreator.value.resourceId
      findProjectsWithViewings shouldBe Set(project.resourceId, otherProject.resourceId)
      findPersonViewings       shouldBe Set(personId -> project.resourceId, personId -> otherProject.resourceId)

      val event = ProjectViewingDeletion(project.path)

      remover.removeViewing(event).unsafeRunSync() shouldBe ()

      findProjectsWithViewings shouldBe Set(otherProject.resourceId)
      findPersonViewings       shouldBe Set(personId -> otherProject.resourceId)
    }

    "remove all person data from the PersonViewing graph " +
      "if he had viewings only from the project that is gone" in new TestCase {

        val personGLId = personGitLabIds.generateOne
        val project    = generateProjectWithCreator(personGLId)
        insertViewing(project, personGLId)

        val otherPersonGLId = personGitLabIds.generateOne
        val otherProject    = generateProjectWithCreator(otherPersonGLId)
        insertViewing(otherProject, otherPersonGLId)

        val personId      = project.maybeCreator.value.resourceId
        val otherPersonId = otherProject.maybeCreator.value.resourceId
        findProjectsWithViewings shouldBe Set(project.resourceId, otherProject.resourceId)
        findPersonViewings       shouldBe Set(personId -> project.resourceId, otherPersonId -> otherProject.resourceId)

        val event = ProjectViewingDeletion(project.path)

        remover.removeViewing(event).unsafeRunSync() shouldBe ()

        findProjectsWithViewings             shouldBe Set(otherProject.resourceId)
        findPersonViewings                   shouldBe Set(otherPersonId -> otherProject.resourceId)
        countPersonViewingsTriples(personId) shouldBe 0
      }

    "do nothing if there's no project with the given path" in new TestCase {

      findProjectsWithViewings shouldBe Set.empty

      val event = ProjectViewingDeletion(projectPaths.generateOne)

      remover.removeViewing(event).unsafeRunSync() shouldBe ()

      findProjectsWithViewings shouldBe Set.empty
    }

    "do nothing if there are no triples in the PersonViewing graph for the given path" in new TestCase {

      val personGLId = personGitLabIds.generateOne
      val project    = generateProjectWithCreator(personGLId)

      upload(to = projectsDataset, project)

      val projectViewedEvent = projectViewedEvents.generateOne.copy(path = project.path, maybeUserId = None)

      eventPersister.persist(projectViewedEvent).unsafeRunSync()

      findProjectsWithViewings shouldBe Set(project.resourceId)
      findPersonViewings       shouldBe Set.empty

      val viewingDeletionEvent = ProjectViewingDeletion(project.path)

      remover.removeViewing(viewingDeletionEvent).unsafeRunSync() shouldBe ()

      findProjectsWithViewings                                          shouldBe Set.empty
      findPersonViewings                                                shouldBe Set.empty
      countPersonViewingsTriples(project.maybeCreator.value.resourceId) shouldBe 0
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val remover = new ViewingRemoverImpl[IO](TSClient[IO](projectsDSConnectionInfo))

    private val tsClient = TSClient[IO](projectsDSConnectionInfo)
    val eventPersister   = new EventPersisterImpl[IO](tsClient, PersonViewedProjectPersister[IO](tsClient))

    def insertViewing(project: Project, userId: persons.GitLabId): Unit = {

      upload(to = projectsDataset, project)

      val event = projectViewedEvents.generateOne.copy(path = project.path, maybeUserId = userId.some)

      eventPersister.persist(event).unsafeRunSync()
    }
  }

  private def generateProjectWithCreator(userId: persons.GitLabId) =
    anyProjectEntities
      .map(replaceProjectCreator(personEntities(userId.some).generateSome))
      .generateOne

  private def findProjectsWithViewings =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find project viewing",
        Prefixes of renku -> "renku",
        sparql"""|SELECT DISTINCT ?id
                 |FROM ${GraphClass.ProjectViewedTimes.id} {
                 |  ?id a renku:ProjectViewedTime
                 |}
                 |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => projects.ResourceId(row("id")))
      .toSet

  private def findPersonViewings =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find person viewing",
        Prefixes of renku -> "renku",
        sparql"""|SELECT DISTINCT ?userId ?projectId
                 |FROM ${GraphClass.PersonViewings.id} {
                 |  ?userId a renku:PersonViewing;
                 |          renku:viewedProject ?viewingId.
                 |  ?viewingId renku:project ?projectId.
                 |}
                 |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => persons.ResourceId(row("userId")) -> projects.ResourceId(row("projectId")))
      .toSet

  private def countPersonViewingsTriples(personId: persons.ResourceId) =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find person viewing triples count",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?s ?p ?o
                 |FROM ${GraphClass.PersonViewings.id} {
                 |  BIND(${personId.asEntityId} AS ?s)
                 |  ?s ?p ?o
                 |}
                 |""".stripMargin
      )
    ).unsafeRunSync().size
}
