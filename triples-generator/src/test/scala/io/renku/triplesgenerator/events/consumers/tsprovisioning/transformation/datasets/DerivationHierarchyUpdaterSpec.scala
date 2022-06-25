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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation
package datasets

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets.TopmostDerivedFrom
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.consumers.tsprovisioning.ProjectFunctions
import io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.Generators.queriesGen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class DerivationHierarchyUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "fixDerivationHierarchies" should {

    "update topmostDerivedFrom on all derivation hierarchies " +
      "and remove existing wasDerivedFrom and topmostDerivedFrom" in new TestCase {

        val (_ ::~ modification1, project) =
          anyRenkuProjectEntities.addDatasetAndModification(datasetEntities(provenanceInternal)).generateOne
        val (_, projectUpdate2) = project.addDataset(
          modification1.createModification().modify(topmostDerivedFromFromDerivedFrom(modification1))
        )
        val finalProject = projectUpdate2.to[entities.Project]

        // at this stage topmostDerivedFrom should be the same as derivedFrom
        val topDS :: mod1DS :: mod2DS :: Nil = finalProject.datasets
        mod1DS.provenance.topmostDerivedFrom shouldBe TopmostDerivedFrom(topDS.identification.resourceId.show)
        mod2DS.provenance.topmostDerivedFrom shouldBe TopmostDerivedFrom(mod1DS.identification.resourceId.show)

        def dataset(expectedDS: entities.Dataset[entities.Dataset.Provenance]) = where {
          (ds: entities.Dataset[entities.Dataset.Provenance.Modified]) =>
            ds.identification.identifier == expectedDS.identification.identifier &&
            ds.provenance.topmostDerivedFrom == topDS.provenance.topmostDerivedFrom
        }

        val mod1DSDerivedQueries = sparqlQueries.generateList()
        (updatesCreator
          .deleteOtherDerivedFrom(_: entities.Dataset[entities.Dataset.Provenance.Modified]))
          .expects(dataset(mod1DS))
          .returning(mod1DSDerivedQueries)
        val mod1DSTopmostQueries = sparqlQueries.generateList()
        (updatesCreator
          .deleteOtherTopmostDerivedFrom(_: entities.Dataset[entities.Dataset.Provenance.Modified]))
          .expects(dataset(mod1DS))
          .returning(mod1DSTopmostQueries)
        val mod2DSDerivedQueries = sparqlQueries.generateList()
        (updatesCreator
          .deleteOtherDerivedFrom(_: entities.Dataset[entities.Dataset.Provenance.Modified]))
          .expects(dataset(mod2DS))
          .returning(mod2DSDerivedQueries)
        val mod2DSTopmostQueries = sparqlQueries.generateList()
        (updatesCreator
          .deleteOtherTopmostDerivedFrom(_: entities.Dataset[entities.Dataset.Provenance.Modified]))
          .expects(dataset(mod2DS))
          .returning(mod2DSTopmostQueries)

        val Success(updatedProject -> queries) = updater.fixDerivationHierarchies(finalProject -> initialQueries)

        val updatedTopDS :: updatedMod1DS :: updatedMod2DS :: Nil = updatedProject.datasets
        updatedTopDS.provenance.topmostDerivedFrom  shouldBe topDS.provenance.topmostDerivedFrom
        updatedMod1DS.provenance.topmostDerivedFrom shouldBe updatedTopDS.provenance.topmostDerivedFrom
        updatedMod2DS.provenance.topmostDerivedFrom shouldBe updatedTopDS.provenance.topmostDerivedFrom

        queries.preDataUploadQueries shouldBe initialQueries.preDataUploadQueries
        queries.postDataUploadQueries shouldBe initialQueries.postDataUploadQueries :::
          mod1DSDerivedQueries ::: mod1DSTopmostQueries ::: mod2DSDerivedQueries ::: mod2DSTopmostQueries
      }
  }

  private trait TestCase {
    val initialQueries = queriesGen.generateOne
    val updatesCreator = mock[UpdatesCreator]
    val updater        = new DerivationHierarchyUpdaterImpl[Try](updatesCreator, ProjectFunctions)

    def topmostDerivedFromFromDerivedFrom(
        otherDs: Dataset[Dataset.Provenance.Modified]
    ): Dataset[Dataset.Provenance.Modified] => Dataset[Dataset.Provenance.Modified] =
      ds => ds.copy(provenance = ds.provenance.copy(topmostDerivedFrom = TopmostDerivedFrom(otherDs.entityId)))
  }
}
