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

package io.renku.entities.searchgraphs.datasets

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class DatasetsCollectorSpec extends AnyWordSpec with should.Matchers with TableDrivenPropertyChecks {

  "collectLastVersions" should {

    "collect no Datasets if there's any" in {
      val project = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      DatasetsCollector.collectLastVersions(project) shouldBe Nil
    }

    "collect no Datasets for a Non-Renku Project" in {
      val project = anyNonRenkuProjectEntities.generateOne.to[entities.Project]
      DatasetsCollector.collectLastVersions(project) shouldBe Nil
    }

    forAll {
      Table(
        "provenance gen"           -> "DS type",
        provenanceInternal         -> "Internal",
        provenanceImportedExternal -> "Imported External",
        provenanceImportedInternal -> "Imported Internal"
      )
    } { (provenanceGen, dsType) =>
      s"collect $dsType Datasets that have no modifications" in {
        val project = anyRenkuProjectEntities
          .withDatasets(datasetEntities(provenanceGen), datasetEntities(provenanceGen))
          .generateOne
          .to[entities.RenkuProject]

        DatasetsCollector.collectLastVersions(project) shouldBe project.datasets
      }

      s"collect datasets that are modifications of $dsType Dataset" in {
        val (modifiedDS, project) = anyRenkuProjectEntities
          .addDatasetAndModification(datasetEntities(provenanceGen))
          .generateOne
          .bimap(_._2.to[entities.Dataset[entities.Dataset.Provenance.Modified]], _.to[entities.RenkuProject])

        DatasetsCollector.collectLastVersions(project) shouldBe List(modifiedDS)
      }
    }

    "collect datasets that are the last modifications of a Dataset" in {
      val project = anyRenkuProjectEntities
        .withDatasets(datasetEntities(provenanceNonModified))
        .modify { p =>
          val firstModifications  = p.datasets.map(_.createModification()(p.dateCreated).generateOne)
          val secondModifications = firstModifications.map(_.createModification()(p.dateCreated).generateOne)
          p.addDatasets(firstModifications ::: secondModifications: _*)
        }
        .generateOne
        .to[entities.RenkuProject]

      DatasetsCollector.collectLastVersions(project) shouldBe List(project.datasets.last)
    }

    "skip datasets that are invalidated" in {

      val project = anyRenkuProjectEntities
        .withDatasets(datasetEntities(provenanceNonModified))
        .modify { p =>
          val modifications = p.datasets.map(_.createModification()(p.dateCreated).generateOne)
          val invalidations = modifications.map(_.invalidateNow(personEntities))
          p.addDatasets(modifications ::: invalidations: _*)
        }
        .generateOne
        .to[entities.RenkuProject]

      DatasetsCollector.collectLastVersions(project) shouldBe Nil
    }
  }
}
