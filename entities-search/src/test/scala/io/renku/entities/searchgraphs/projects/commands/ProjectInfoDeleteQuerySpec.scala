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

package io.renku.entities.searchgraphs
package projects.commands

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities}
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger

class ProjectInfoDeleteQuerySpec
    extends AnyWordSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with IOSpec {

  "the delete query" should {

    "generate query that removes data of a single project info" in {

      val project1 = anyProjectEntities.generateOne.to[entities.Project]
      val project2 = anyProjectEntities.generateOne.to[entities.Project]

      IOBody {
        insertSearchInfo(project1) >>
          insertSearchInfo(project2) >>
          findProjects.map(_ should contain only (project1.resourceId, project2.resourceId)) >>
          runUpdate(on = projectsDataset, ProjectInfoDeleteQuery(project1.resourceId)) >>
          findProjects.map(_ shouldBe List(project2.resourceId))
      }
    }

    "generate query that removes all the data of the project" in {

      val project = anyProjectEntities.generateOne.to[entities.Project]

      IOBody {
        insertSearchInfo(project) >>
          findProjects.map(_ shouldBe List(project.resourceId)) >>
          runUpdate(on = projectsDataset, ProjectInfoDeleteQuery(project.resourceId)) >>
          triplesCount(on = projectsDataset, graphId = GraphClass.Projects.id).map(_ shouldBe 0L)
      }
    }
  }

  implicit override val ioLogger: Logger[IO] = TestLogger[IO]()

  private def findProjects = runSelect(
    on = projectsDataset,
    SparqlQuery.ofUnsafe(
      "find Discoverable Projects",
      Prefixes of renku -> "renku",
      sparql"""|SELECT DISTINCT ?id
               |WHERE {
               |  GRAPH ${GraphClass.Projects.id} {
               |    ?id a renku:DiscoverableProject
               |  }
               |}
               |""".stripMargin
    )
  ).map(_.map(row => model.projects.ResourceId(row("id"))))
}
