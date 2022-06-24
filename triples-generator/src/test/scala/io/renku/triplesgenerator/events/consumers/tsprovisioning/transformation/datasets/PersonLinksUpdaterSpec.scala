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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.datasets

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.personResourceIds
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.Generators.queriesGen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class PersonLinksUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "updatePersonLinks" should {

    "create queries that unlinks the DS creators" in new TestCase {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      val creatorsInKG = personResourceIds.generateSet()
      (kgDatasetInfoFinder.findDatasetCreators _).expects(ds.resourceId).returning(creatorsInKG.pure[Try])

      val unlinkingQueries = sparqlQueries.generateList()
      (updatesCreator.queriesUnlinkingCreators _).expects(ds, creatorsInKG).returning(unlinkingQueries)

      val Success(updatedProject -> queries) = updater.updatePersonLinks(project -> initialQueries)

      updatedProject                shouldBe project
      queries.preDataUploadQueries  shouldBe initialQueries.preDataUploadQueries ::: unlinkingQueries
      queries.postDataUploadQueries shouldBe initialQueries.postDataUploadQueries
    }
  }

  private trait TestCase {
    val initialQueries      = queriesGen.generateOne
    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]
    val updatesCreator      = mock[UpdatesCreator]
    val updater             = new PersonLinksUpdaterImpl[Try](kgDatasetInfoFinder, updatesCreator)
  }
}
