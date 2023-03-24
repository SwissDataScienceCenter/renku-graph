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
import collector.projects.viewed.{EventPersisterImpl, PersonViewedProjectPersister}
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{projects, GraphClass}
import io.renku.graph.model.testentities._
import io.renku.graph.model.Schemas.renku
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.ProjectViewingDeletion
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ViewingRemoverSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "removeViewing" should {

    "remove the relevant renku:ProjectViewedTime entity from the ProjectViewedTimes graph" in new TestCase {

      val project = anyProjectEntities.generateOne
      insertViewing(project)

      val otherProject = anyProjectEntities.generateOne
      insertViewing(otherProject)

      findProjectsWithViewings shouldBe Set(project.resourceId, otherProject.resourceId)

      val event = ProjectViewingDeletion(project.path)

      remover.removeViewing(event).unsafeRunSync() shouldBe ()

      findProjectsWithViewings shouldBe Set(otherProject.resourceId)
    }

    "do nothing if there's no project with the given path" in new TestCase {

      findProjectsWithViewings shouldBe Set.empty

      val event = ProjectViewingDeletion(projectPaths.generateOne)

      remover.removeViewing(event).unsafeRunSync() shouldBe ()

      findProjectsWithViewings shouldBe Set.empty
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val remover = new ViewingRemoverImpl[IO](TSClient[IO](projectsDSConnectionInfo))

    private val tsClient       = TSClient[IO](projectsDSConnectionInfo)
    private val eventPersister = new EventPersisterImpl[IO](tsClient, PersonViewedProjectPersister[IO](tsClient))

    def insertViewing(project: Project) = {

      upload(to = projectsDataset, project)

      val event = projectViewedEvents.generateOne.copy(path = project.path)

      eventPersister.persist(event).unsafeRunSync()
    }
  }

  private def findProjectsWithViewings =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find project viewing",
        Prefixes of renku -> "renku",
        s"""|SELECT DISTINCT ?id
            |FROM ${GraphClass.ProjectViewedTimes.id.asSparql.sparql} {
            |  ?id a renku:ProjectViewedTime
            |}
            |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => projects.ResourceId(row("id")))
      .toSet
}
