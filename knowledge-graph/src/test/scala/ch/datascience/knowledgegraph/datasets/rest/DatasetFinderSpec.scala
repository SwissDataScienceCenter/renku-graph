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
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "findDataset" should {

    "return details of the dataset with the given id " +
      "- a case of a non-modified renku dataset used in a single project" in new TestCase {

        forAll(datasetEntities(provenanceGen = datasetProvenanceInternal)) { dataset =>
          loadToStore(
            dataset.asJsonLD,
            datasetEntities(ofAnyProvenance).generateOne.asJsonLD
          )

          datasetFinder
            .findDataset(dataset.identifier)
            .unsafeRunSync() shouldBe dataset.to[NonModifiedDataset].some
        }
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same imported dataset" in new TestCase {
        val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne

        loadToStore(
          dataset1.asJsonLD,
          dataset2.asJsonLD,
          datasetEntities(ofAnyProvenance).generateOne.asJsonLD
        )

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync() shouldBe Some(
          dataset1
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset1.project.path, dataset1.project.name),
                DatasetProject(dataset2.project.path, dataset2.project.name)
              )
            )
        )

        datasetFinder.findDataset(dataset2.identifier).unsafeRunSync() shouldBe Some(
          dataset2
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset1.project.path, dataset1.project.name),
                DatasetProject(dataset2.project.path, dataset2.project.name)
              )
            )
        )
      }

    "return details of the dataset with the given id " +
      "- a case when dataset is modified" in new TestCase {
        val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne

        val modifiedDatasetOnProject1 = modifiedDatasetEntities(dataset1).generateOne

        loadToStore(
          dataset1.asJsonLD,
          dataset2.asJsonLD,
          modifiedDatasetOnProject1.asJsonLD
        )

        datasetFinder
          .findDataset(dataset2.identifier)
          .unsafeRunSync() shouldBe dataset2.to[NonModifiedDataset].some

        datasetFinder
          .findDataset(modifiedDatasetOnProject1.identifier)
          .unsafeRunSync() shouldBe modifiedDatasetOnProject1.to[ModifiedDataset].some
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset created in a Renku project" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { sourceDataset =>
          val datasetOnProject1 = sourceDataset.importTo(projectEntities().generateOne)
          val datasetOnProject2 = sourceDataset.importTo(projectEntities().generateOne)

          loadToStore(
            sourceDataset.asJsonLD,
            datasetOnProject1.asJsonLD,
            datasetOnProject2.asJsonLD,
            datasetEntities(ofAnyProvenance).generateOne.asJsonLD
          )

          datasetFinder.findDataset(sourceDataset.identifier).unsafeRunSync() shouldBe
            sourceDataset
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  DatasetProject(sourceDataset.project.path, sourceDataset.project.name),
                  DatasetProject(datasetOnProject1.project.path, datasetOnProject1.project.name),
                  DatasetProject(datasetOnProject2.project.path, datasetOnProject2.project.name)
                )
              )
              .some

          datasetFinder.findDataset(datasetOnProject2.identifier).unsafeRunSync() shouldBe
            datasetOnProject2
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  DatasetProject(sourceDataset.project.path, sourceDataset.project.name),
                  DatasetProject(datasetOnProject1.project.path, datasetOnProject1.project.name),
                  DatasetProject(datasetOnProject2.project.path, datasetOnProject2.project.name)
                )
              )
              .some
        }
      }

    "return None if there are no datasets with the given id" in new TestCase {
      datasetFinder.findDataset(datasetIdentifiers.generateOne).unsafeRunSync() shouldBe None
    }
  }

  "findDataset in case of forks" should {

    "return details of the dataset with the given id " +
      "- a case when a Renku created dataset is defined on a project which has a fork" in new TestCase {

        forAll(datasetEntities(datasetProvenanceInternal)) { originalDataset =>
          val datasetOnForkedProject = originalDataset.fork()

          loadToStore(
            originalDataset.asJsonLD,
            datasetOnForkedProject.asJsonLD
          )

          datasetFinder.findDataset(originalDataset.identifier).unsafeRunSync() shouldBe
            originalDataset
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  DatasetProject(originalDataset.project.path, originalDataset.project.name),
                  DatasetProject(datasetOnForkedProject.project.path, datasetOnForkedProject.project.name)
                )
              )
              .some

          datasetFinder.findDataset(datasetOnForkedProject.identifier).unsafeRunSync() shouldBe
            datasetOnForkedProject
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  DatasetProject(originalDataset.project.path, originalDataset.project.name),
                  DatasetProject(datasetOnForkedProject.project.path, datasetOnForkedProject.project.name)
                )
              )
              .some
        }
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are sharing a dataset and one of the projects is forked" in new TestCase {
        val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne
        val dataset2Fork                = dataset2.fork()

        loadToStore(
          dataset1.asJsonLD,
          dataset2.asJsonLD,
          dataset2Fork.asJsonLD
        )

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync() shouldBe
          dataset1
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset1.project.path, dataset1.project.name),
                DatasetProject(dataset2.project.path, dataset2.project.name),
                DatasetProject(dataset2Fork.project.path, dataset2Fork.project.name)
              )
            )
            .some

        datasetFinder.findDataset(dataset2.identifier).unsafeRunSync() shouldBe
          dataset2
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset1.project.path, dataset1.project.name),
                DatasetProject(dataset2.project.path, dataset2.project.name),
                DatasetProject(dataset2Fork.project.path, dataset2Fork.project.name)
              )
            )
            .some
      }

    "return details of the dataset with the given id " +
      "- a case when a Renku created dataset is defined on a grandparent project with two levels of forks" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { grandparentDataset =>
          val parentDataset = grandparentDataset.fork()
          val childDataset  = parentDataset.fork()

          loadToStore(
            grandparentDataset.asJsonLD,
            parentDataset.asJsonLD,
            childDataset.asJsonLD
          )

          datasetFinder.findDataset(grandparentDataset.identifier).unsafeRunSync() shouldBe
            grandparentDataset
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  DatasetProject(grandparentDataset.project.path, grandparentDataset.project.name),
                  DatasetProject(parentDataset.project.path, parentDataset.project.name),
                  DatasetProject(childDataset.project.path, childDataset.project.name)
                )
              )
              .some
        }
      }

    "return details of the modified dataset with the given id " +
      "- case when modification is followed by forking" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { originalDataset =>
          val modifiedDataset     = modifiedDatasetEntities(originalDataset).generateOne
          val modifiedDatasetFork = modifiedDataset.fork()

          loadToStore(
            originalDataset.asJsonLD,
            modifiedDataset.asJsonLD,
            modifiedDatasetFork.asJsonLD
          )

          datasetFinder.findDataset(originalDataset.identifier).unsafeRunSync() shouldBe Some(
            originalDataset.to[NonModifiedDataset].copy(usedIn = Nil)
          )

          datasetFinder.findDataset(modifiedDataset.identifier).unsafeRunSync() shouldBe Some(
            modifiedDataset
              .to[ModifiedDataset]
              .copy(
                usedIn = List(
                  DatasetProject(modifiedDataset.project.path, modifiedDataset.project.name),
                  DatasetProject(modifiedDatasetFork.project.path, modifiedDatasetFork.project.name)
                )
              )
          )
        }
      }

    "return details of the dataset with the given id " +
      "- case when forking is followed by modification" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { originalDataset =>
          val datasetForked   = originalDataset.fork()
          val modifiedDataset = modifiedDatasetEntities(datasetForked).generateOne

          loadToStore(
            originalDataset.asJsonLD,
            datasetForked.asJsonLD,
            modifiedDataset.asJsonLD
          )

          datasetFinder.findDataset(originalDataset.identifier).unsafeRunSync() shouldBe Some(
            originalDataset.to[NonModifiedDataset]
          )

          datasetFinder.findDataset(modifiedDataset.identifier).unsafeRunSync() shouldBe Some(
            modifiedDataset.to[ModifiedDataset]
          )
        }
      }

    "return details of the dataset with the given id " +
      "- case when a dataset on a fork is deleted" in new TestCase {
        fail("implementation not known yet")
      }

    "return details of a fork dataset with the given id " +
      "- case when the parent of a fork dataset is deleted" in new TestCase {
        fail("implementation not known yet")
      }
  }

  "findDataset in the case of dataset import hierarchy" should {

    "return details of the dataset with the given id " +
      "- case when the first dataset is imported from a third party provider" in new TestCase {
        val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne
        val dataset3                    = dataset2.importTo(projectEntities().generateOne)

        loadToStore(
          dataset1.asJsonLD,
          dataset2.asJsonLD,
          dataset3.asJsonLD
        )

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync() shouldBe Some(
          dataset1
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset1.project.path, dataset1.project.name),
                DatasetProject(dataset2.project.path, dataset2.project.name),
                DatasetProject(dataset3.project.path, dataset3.project.name)
              )
            )
        )

        datasetFinder.findDataset(dataset3.identifier).unsafeRunSync() shouldBe Some(
          dataset3
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset1.project.path, dataset1.project.name),
                DatasetProject(dataset2.project.path, dataset2.project.name),
                DatasetProject(dataset3.project.path, dataset3.project.name)
              )
            )
        )
      }

    "return details of the dataset with the given id " +
      "- case when the first dataset is renku created" in new TestCase {

        val dataset1 = datasetEntities(datasetProvenanceInternal).generateOne
        val dataset2 = dataset1.importTo(projectEntities().generateOne)
        val dataset3 = dataset2.importTo(projectEntities().generateOne)

        loadToStore(
          dataset1.asJsonLD,
          dataset2.asJsonLD,
          dataset3.asJsonLD
        )

        datasetFinder
          .findDataset(dataset1.identifier)
          .unsafeRunSync() shouldBe Some(
          dataset1
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset1.project.path, dataset1.project.name),
                DatasetProject(dataset2.project.path, dataset2.project.name),
                DatasetProject(dataset3.project.path, dataset3.project.name)
              )
            )
        )

        datasetFinder
          .findDataset(dataset3.identifier)
          .unsafeRunSync() shouldBe Some(
          dataset3
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset1.project.path, dataset1.project.name),
                DatasetProject(dataset2.project.path, dataset2.project.name),
                DatasetProject(dataset3.project.path, dataset3.project.name)
              )
            )
        )
      }

    "return details of the dataset with the given id " +
      "- case when the sameAs hierarchy is broken by dataset modification" in new TestCase {
        val dataset1         = datasetEntities(datasetProvenanceInternal).generateOne
        val dataset2         = dataset1.importTo(projectEntities().generateOne)
        val dataset2Modified = modifiedDatasetEntities(dataset2).generateOne
        val dataset3         = dataset2Modified.importTo(projectEntities().generateOne)

        loadToStore(
          dataset1.asJsonLD,
          dataset2.asJsonLD,
          dataset2Modified.asJsonLD,
          dataset3.asJsonLD
        )

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync() shouldBe Some(dataset1.to[NonModifiedDataset])
        datasetFinder.findDataset(dataset2Modified.identifier).unsafeRunSync() shouldBe Some(
          dataset2Modified
            .to[ModifiedDataset]
            .copy(
              usedIn = List(
                DatasetProject(dataset2Modified.project.path, dataset2Modified.project.name),
                DatasetProject(dataset3.project.path, dataset3.project.name)
              )
            )
        )
      }

    "not return the details of a dataset" +
      "- case when the dataset has been invalidated" in new TestCase {
        fail("implementation not known yet")
      }

    "not return the details of a dataset" +
      "- case when the latest version of the dataset has been invalidated" in new TestCase {
        fail("implementation not known yet")
      }
  }

  private trait TestCase {
    implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetFinder = new IODatasetFinder(
      new BaseDetailsFinder(rdfStoreConfig, logger, timeRecorder),
      new CreatorsFinder(rdfStoreConfig, logger, timeRecorder),
      new PartsFinder(rdfStoreConfig, logger, timeRecorder),
      new ProjectsFinder(rdfStoreConfig, logger, timeRecorder)
    )
  }
}
