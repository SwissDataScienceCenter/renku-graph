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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation
package datasets

import Generators.queriesGen
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.datasetCreatedDates
import io.renku.graph.model.datasets.DateCreated
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.rdfstore.SparqlQuery
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class DateCreatedUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "updateDateCreated" should {

    "prepare updates for changes to dateCreated" in new TestCase {
      val (_, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(identity, _.to[entities.Project])

      val datasets = project.datasets

      val allUpdateQueries = datasets.foldRight(List.empty[SparqlQuery]) { (ds, allQueries) =>
        val updateQueries = sparqlQueries.generateList()
        givenDateCreatedUpdates(ds, updateQueries = updateQueries)
        updateQueries ::: allQueries
      }

      val Success(updatedProject -> queries) = updater.updateDateCreated(project -> initialQueries)

      updatedProject                shouldBe project
      queries.preDataUploadQueries  shouldBe initialQueries.preDataUploadQueries ::: allUpdateQueries
      queries.postDataUploadQueries shouldBe initialQueries.postDataUploadQueries
    }
  }

  private trait TestCase {
    val initialQueries      = queriesGen.generateOne
    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]
    val updatesCreator      = mock[UpdatesCreator]
    val updater             = new DateCreatedUpdaterImpl[Try](kgDatasetInfoFinder, updatesCreator)

    def givenDateCreatedUpdates(ds:                  entities.Dataset[entities.Dataset.Provenance],
                                existingDateCreated: Set[DateCreated] = datasetCreatedDates().generateSet(),
                                updateQueries:       List[SparqlQuery] = sparqlQueries.generateList()
    ): Unit = {
      (kgDatasetInfoFinder.findDatasetDateCreated _)
        .expects(ds.resourceId)
        .returning(existingDateCreated.pure[Try])
      (updatesCreator.removeOtherDateCreated _)
        .expects(ds, existingDateCreated)
        .returning(updateQueries)
      ()
    }
  }
}
