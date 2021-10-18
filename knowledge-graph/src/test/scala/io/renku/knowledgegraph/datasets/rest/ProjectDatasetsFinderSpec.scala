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

package io.renku.knowledgegraph.datasets.rest

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.{InitialVersion, SameAs}
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.stubbing.ExternalServiceStubbing
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
      val (original ::~ modification1, project) =
        projectEntities(anyVisibility).addDatasetAndModification(datasetEntities(provenanceInternal)).generateOne
      val (modification2, projectComplete) = project.addDataset(modification1.createModification())

      loadToStore(
        projectEntities(anyVisibility).addDataset(datasetEntities(provenanceNonModified)).generateOne._2,
        projectComplete
      )

      datasetsFinder
        .findProjectDatasets(projectComplete.path)
        .unsafeRunSync() shouldBe List(
        (modification2.identification.identifier,
         InitialVersion(original.identification.identifier),
         modification2.identification.title,
         modification2.identification.name,
         modification2.provenance.derivedFrom.asRight,
         modification2.additionalInfo.images
        )
      )
    }

    "return non-modified datasets and the very last modifications of project's datasets" in new TestCase {
      val (dataset1 ::~ dataset2 ::~ modified2, project) = projectEntities(anyVisibility)
        .addDataset(datasetEntities(provenanceImportedExternal))
        .addDatasetAndModification(datasetEntities(provenanceInternal))
        .generateOne

      loadToStore(project)

      datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() shouldBe List(
        (dataset1.identification.identifier,
         InitialVersion(dataset1.identification.identifier),
         dataset1.identification.title,
         dataset1.identification.name,
         dataset1.provenance.sameAs.asLeft,
         dataset1.additionalInfo.images
        ),
        (modified2.identification.identifier,
         InitialVersion(dataset2.identification.identifier),
         modified2.identification.title,
         modified2.identification.name,
         modified2.provenance.derivedFrom.asRight,
         modified2.additionalInfo.images
        )
      ).sortBy(_._3)
    }

    "return all datasets of the given project without merging datasets having the same sameAs" in new TestCase {
      val (original, originalProject) =
        anyProjectEntities.addDataset(datasetEntities(provenanceInternal)).generateOne
      val (dataset1 ::~ dataset2, project) =
        anyProjectEntities.importDataset(original).importDataset(original).generateOne

      assume(dataset1.provenance.topmostSameAs == dataset2.provenance.topmostSameAs)
      assume(dataset1.provenance.topmostSameAs == original.provenance.topmostSameAs)

      loadToStore(originalProject, project)

      datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() should contain theSameElementsAs List(
        (dataset1.identification.identifier,
         InitialVersion(dataset1.identification.identifier),
         dataset1.identification.title,
         original.identification.name,
         dataset1.provenance.sameAs.asLeft,
         original.additionalInfo.images
        ),
        (dataset2.identification.identifier,
         InitialVersion(dataset2.identification.identifier),
         dataset2.identification.title,
         original.identification.name,
         dataset2.provenance.sameAs.asLeft,
         original.additionalInfo.images
        )
      )
    }

    "return None if there are no datasets in the project" in new TestCase {
      datasetsFinder.findProjectDatasets(projectPaths.generateOne).unsafeRunSync() shouldBe List.empty
    }

    "not returned deleted dataset" in new TestCase {
      val (_ ::~ _ ::~ dataset2, project) = projectEntities(anyVisibility)
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne

      loadToStore(project)

      datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() shouldBe List(
        (dataset2.identification.identifier,
         InitialVersion(dataset2.identification.identifier),
         dataset2.identification.title,
         dataset2.identification.name,
         SameAs(dataset2.provenance.topmostSameAs.value).asLeft,
         dataset2.additionalInfo.images
        )
      )
    }

    "not returned deleted dataset when its latest version was deleted" in new TestCase {
      val (_ ::~ modification, project) =
        projectEntities(anyVisibility).addDatasetAndModification(datasetEntities(provenanceInternal)).generateOne

      loadToStore(project.addDatasets(modification.invalidateNow))

      datasetsFinder.findProjectDatasets(project.path).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetsFinder       = new ProjectDatasetsFinderImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }
}
