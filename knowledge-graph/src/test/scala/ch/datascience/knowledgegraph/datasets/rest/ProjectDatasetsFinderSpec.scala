/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.InitialVersion
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectDatasetsFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findProjectDatasets" should {

    "return the very last modification of a dataset for the given project" in new TestCase {
      forAll(datasetEntities(datasetProvenanceInternal)) { originalDataset =>
        val datasetModification1 = modifiedDatasetEntities(originalDataset).generateOne
        val datasetModification2 = modifiedDatasetEntities(datasetModification1).generateOne

        loadToStore(
          datasetEntities(ofAnyProvenance).generateOne,
          originalDataset,
          datasetModification1,
          datasetModification2
        )

        datasetsFinder
          .findProjectDatasets(originalDataset.project.path)
          .unsafeRunSync() shouldBe List(
          (datasetModification2.identification.identifier,
           InitialVersion(originalDataset.identification.identifier),
           datasetModification2.identification.title,
           datasetModification2.identification.name,
           datasetModification2.provenance.derivedFrom.asRight,
           datasetModification2.additionalInfo.images
          )
        )
      }
    }

    "return non-modified datasets and the very last modifications of project's datasets" in new TestCase {
      forAll(datasetEntities(datasetProvenanceImportedExternal)) { dataset1 =>
        val dataset2             = datasetEntities(datasetProvenanceInternal, projectsGen = fixed(dataset1.project)).generateOne
        val dataset2Modification = modifiedDatasetEntities(dataset2).generateOne

        loadToStore(
          dataset1,
          dataset2,
          dataset2Modification
        )

        datasetsFinder.findProjectDatasets(dataset1.project.path).unsafeRunSync() shouldBe List(
          (dataset1.identification.identifier,
           InitialVersion(dataset1.identification.identifier),
           dataset1.identification.title,
           dataset1.identification.name,
           dataset1.provenance.sameAs.asLeft,
           dataset1.additionalInfo.images
          ),
          (dataset2Modification.identification.identifier,
           InitialVersion(dataset2.identification.identifier),
           dataset2Modification.identification.title,
           dataset2Modification.identification.name,
           dataset2Modification.provenance.derivedFrom.asRight,
           dataset2Modification.additionalInfo.images
          )
        ).sortBy(_._3)
      }
    }

    "return all datasets of the given project without merging datasets having the same sameAs" in new TestCase {
      val project                     = projectEntities[Project.ForksCount.Zero](visibilityNonPublic).generateOne
      val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne

      loadToStore(
        datasetEntities(ofAnyProvenance).generateOne,
        dataset1,
        dataset2
      )

      datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() should contain theSameElementsAs List(
        (dataset1.identification.identifier,
         InitialVersion(dataset1.identification.identifier),
         dataset1.identification.title,
         dataset1.identification.name,
         dataset1.provenance.sameAs.asLeft,
         dataset1.additionalInfo.images
        ),
        (dataset2.identification.identifier,
         InitialVersion(dataset2.identification.identifier),
         dataset1.identification.title,
         dataset1.identification.name,
         dataset1.provenance.sameAs.asLeft,
         dataset1.additionalInfo.images
        )
      )
    }

    "return None if there are no datasets in the project" in new TestCase {
      datasetsFinder.findProjectDatasets(projectPaths.generateOne).unsafeRunSync() shouldBe List.empty
    }

    "not returned deleted dataset" in new TestCase {
      fail("implementation not known yet")
    }

    "not returned deleted dataset when its latest version was deleted" in new TestCase {
      fail("implementation not known yet")
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetsFinder       = new IOProjectDatasetsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }
}
