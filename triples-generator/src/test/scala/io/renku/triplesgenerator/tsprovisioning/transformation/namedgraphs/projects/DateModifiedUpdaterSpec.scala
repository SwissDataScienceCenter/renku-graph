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

package io.renku.triplesgenerator.tsprovisioning.transformation
package namedgraphs.projects

import TestDataTools._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.entities
import io.renku.graph.model.entities.{NonRenkuProject, RenkuProject}
import io.renku.graph.model.projects.DateModified
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.tsprovisioning.Generators.queriesGen
import io.renku.triplesstore.SparqlQuery
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class DateModifiedUpdaterSpec extends AnyFlatSpec with should.Matchers with MockFactory {

  it should "prepare dateModified deletion queries and do not change the given project " +
    "if project dateModified on the model is > dateModified currently in the TS" in {

      val project = anyProjectEntities.generateOne.to[entities.Project]
      val tsData = toProjectMutableData(project)
        .copy(modifiedDates =
          List(timestamps(max = project.dateModified.value.minusMillis(1)).generateAs(DateModified))
        )

      val updateQueries = sparqlQueries.generateList()
      givenDateModifiedUpdates(project, tsData, updateQueries)

      val updatedProject -> queries = updater.updateDateModified(tsData)(project -> initialQueries)

      updatedProject                shouldBe project
      queries.preDataUploadQueries  shouldBe initialQueries.preDataUploadQueries ::: updateQueries
      queries.postDataUploadQueries shouldBe initialQueries.postDataUploadQueries
    }

  it should "update the given project's dateModified and do not create any deletion queries " +
    "if project dateModified on the model is < dateModified currently in the TS" in {

      val project = anyProjectEntities.generateOne.to[entities.Project]
      val tsDateModified =
        timestampsNotInTheFuture(butYoungerThan = project.dateModified.value).generateAs(DateModified)
      val tsData = toProjectMutableData(project).copy(modifiedDates = List(tsDateModified))

      val updatedProject -> queries = updater.updateDateModified(tsData)(project -> initialQueries)

      queries shouldBe initialQueries
      updatedProject shouldBe {
        project match {
          case p: RenkuProject.WithoutParent    => p.copy(dateModified = tsDateModified)
          case p: RenkuProject.WithParent       => p.copy(dateModified = tsDateModified)
          case p: NonRenkuProject.WithoutParent => p.copy(dateModified = tsDateModified)
          case p: NonRenkuProject.WithParent    => p.copy(dateModified = tsDateModified)
        }
      }
    }

  it should "do not update the given project's dateModified and do not create any deletion queries " +
    "if both the dates are the same" in {

      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgData  = toProjectMutableData(project)

      updater.updateDateModified(kgData)(project -> initialQueries) shouldBe (project -> initialQueries)
    }

  it should "do not update the given project's dateModified and do not create any deletion queries " +
    "if there's no dateModified in the TS" in {

      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgData  = toProjectMutableData(project).copy(modifiedDates = List.empty)

      updater.updateDateModified(kgData)(project -> initialQueries) shouldBe (project -> initialQueries)
    }

  private lazy val initialQueries = queriesGen.generateOne
  private lazy val updatesCreator = mock[UpdatesCreator]
  private lazy val updater        = new DateModifiedUpdaterImpl(updatesCreator)

  private def givenDateModifiedUpdates(modelProject:  entities.Project,
                                       kgProject:     ProjectMutableData,
                                       updateQueries: List[SparqlQuery] = sparqlQueries.generateList()
  ): Unit = {
    (updatesCreator.dateModifiedDeletion _)
      .expects(modelProject, kgProject)
      .returning(updateQueries)
    ()
  }
}
