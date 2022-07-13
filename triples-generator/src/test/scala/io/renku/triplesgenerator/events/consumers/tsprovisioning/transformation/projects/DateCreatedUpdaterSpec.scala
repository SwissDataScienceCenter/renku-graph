/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning
package transformation
package projects

import Generators.queriesGen
import TestDataTools._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.entities
import io.renku.graph.model.entities.{NonRenkuProject, RenkuProject}
import io.renku.graph.model.projects.DateCreated
import io.renku.graph.model.testentities._
import io.renku.rdfstore.SparqlQuery
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DateCreatedUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "updateDateCreated" should {

    "prepare dateCreated deletion queries and do not change the given project " +
      "if project dateCreated on the model is < dateCreated currently in KG" in new TestCase {
        val project = anyProjectEntities.generateOne.to[entities.Project]
        val kgData = toProjectMutableData(project).copy(dateCreated =
          timestampsNotInTheFuture(butYoungerThan = project.dateCreated.value).generateAs(DateCreated)
        )

        val updateQueries = sparqlQueries.generateList()
        givenDateCreatedUpdates(project, kgData, updateQueries)

        val updatedProject -> queries = updater.updateDateCreated(kgData)(project -> initialQueries)

        updatedProject                shouldBe project
        queries.preDataUploadQueries  shouldBe initialQueries.preDataUploadQueries ::: updateQueries
        queries.postDataUploadQueries shouldBe initialQueries.postDataUploadQueries
      }

    "update the given project's dateCreated and do not create any deletion queries " +
      "if project dateCreated on the model is > dateCreated currently in KG" in new TestCase {
        val project       = anyProjectEntities.generateOne.to[entities.Project]
        val kgDateCreated = timestamps(max = project.dateCreated.value.minusMillis(1)).generateAs(DateCreated)
        val kgData        = toProjectMutableData(project).copy(dateCreated = kgDateCreated)

        val updatedProject -> queries = updater.updateDateCreated(kgData)(project -> initialQueries)

        queries shouldBe initialQueries
        updatedProject shouldBe {
          project match {
            case p: RenkuProject.WithoutParent    => p.copy(dateCreated = kgDateCreated)
            case p: RenkuProject.WithParent       => p.copy(dateCreated = kgDateCreated)
            case p: NonRenkuProject.WithoutParent => p.copy(dateCreated = kgDateCreated)
            case p: NonRenkuProject.WithParent    => p.copy(dateCreated = kgDateCreated)
          }
        }
      }

    "do not update the given project's dateCreated and do not create any deletion queries " +
      "if both the dates are the same" in new TestCase {
        val project = anyProjectEntities.generateOne.to[entities.Project]
        val kgData  = toProjectMutableData(project)

        updater.updateDateCreated(kgData)(project -> initialQueries) shouldBe (project -> initialQueries)
      }
  }

  private trait TestCase {
    val initialQueries = queriesGen.generateOne
    val updatesCreator = mock[UpdatesCreator]
    val updater        = new DateCreatedUpdaterImpl(updatesCreator)

    def givenDateCreatedUpdates(modelProject:  entities.Project,
                                kgProject:     ProjectMutableData,
                                updateQueries: List[SparqlQuery] = sparqlQueries.generateList()
    ): Unit = {
      (updatesCreator.dateCreatedDeletion _)
        .expects(modelProject, kgProject)
        .returning(updateQueries)
      ()
    }
  }
}
