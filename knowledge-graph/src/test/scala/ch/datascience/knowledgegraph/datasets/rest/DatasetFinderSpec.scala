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
import ch.datascience.graph.model.datasets.SameAs
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.Project.ForksCount
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class DatasetFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "findDataset" should {

    "return details of the dataset with the given id " +
      "- a case of a non-modified renku dataset used in a single project" in new TestCase {

        forAll(datasetEntities(datasetProvenanceInternal)) { dataset =>
          loadToStore(dataset, datasetEntities(ofAnyProvenance).generateOne)

          datasetFinder
            .findDataset(dataset.identifier)
            .unsafeRunSync() shouldBe dataset.to[NonModifiedDataset].some
        }
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same imported dataset" in new TestCase {
        val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne

        loadToStore(dataset1, dataset2, datasetEntities(ofAnyProvenance).generateOne)

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync() shouldBe Some(
          dataset1
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                dataset1.project.to[DatasetProject],
                dataset2.project.to[DatasetProject]
              ).sorted
            )
        )

        datasetFinder.findDataset(dataset2.identifier).unsafeRunSync() shouldBe Some(
          dataset2
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                dataset1.project.to[DatasetProject],
                dataset2.project.to[DatasetProject]
              ).sorted
            )
        )
      }

    "return details of the dataset with the given id " +
      "- a case when dataset is modified" in new TestCase {
        val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne

        val modifiedDatasetOnProject1 = modifiedDatasetEntities(dataset1).generateOne

        loadToStore(dataset1, dataset2, modifiedDatasetOnProject1)

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
          val datasetOnProject1 =
            sourceDataset importTo projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne
          val datasetOnProject2 =
            sourceDataset importTo projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne

          loadToStore(
            sourceDataset,
            datasetOnProject1,
            datasetOnProject2,
            datasetEntities(ofAnyProvenance).generateOne
          )

          datasetFinder.findDataset(sourceDataset.identifier).unsafeRunSync() shouldBe
            sourceDataset
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  sourceDataset.project.to[DatasetProject],
                  datasetOnProject1.project.to[DatasetProject],
                  datasetOnProject2.project.to[DatasetProject]
                ).sorted
              )
              .some

          datasetFinder.findDataset(datasetOnProject2.identifier).unsafeRunSync() shouldBe
            datasetOnProject2
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  sourceDataset.project.to[DatasetProject],
                  datasetOnProject1.project.to[DatasetProject],
                  datasetOnProject2.project.to[DatasetProject]
                ).sorted
              )
              .some
        }
      }

    "return None if there are no datasets with the given id" in new TestCase {
      datasetFinder.findDataset(datasetIdentifiers.generateOne).unsafeRunSync() shouldBe None
    }

    "return None if dataset was invalidated" in new TestCase {

      val original = datasetEntities(datasetProvenanceInternal).generateOne
      val invalidated = original
        .invalidate(invalidationTimes(original.provenance.date).generateOne)
        .fold(errors => fail(errors.intercalate("; ")), identity)

      loadToStore(original, invalidated)

      datasetFinder.findDataset(original.identifier).unsafeRunSync()    shouldBe None
      datasetFinder.findDataset(invalidated.identifier).unsafeRunSync() shouldBe None
    }

    "return a dataset without invalidated part" in new TestCase {
      val original = datasetEntities(datasetProvenanceInternal)
        .map(ds => ds.copy(parts = datasetPartEntities(ds.provenance.date.instant).generateNonEmptyList().toList))
        .generateOne
      val partToInvalidate = Random.shuffle(original.parts).head
      val originalWithInvalidatedPart = original
        .invalidatePart(partToInvalidate, invalidationTimes(partToInvalidate.dateCreated).generateOne)
        .fold(errors => fail(errors.intercalate("; ")), identity)

      loadToStore(original, originalWithInvalidatedPart)

      datasetFinder.findDataset(original.identifier).unsafeRunSync() shouldBe Some(
        original.to[NonModifiedDataset].copy(usedIn = Nil)
      )

      datasetFinder.findDataset(originalWithInvalidatedPart.identifier).unsafeRunSync() shouldBe Some(
        originalWithInvalidatedPart
          .copy(parts = original.parts.filterNot(_ == partToInvalidate))
          .to[ModifiedDataset]
      )
    }
  }

  "findDataset in case of forks" should {

    "return details of the dataset with the given id " +
      "- a case when a Renku created dataset is defined on a project which has a fork" in new TestCase {

        forAll(datasetEntities(datasetProvenanceInternal)) { originalDataset =>
          val datasetOnForkedProject = originalDataset.forkProject().fork

          loadToStore(originalDataset, datasetOnForkedProject)

          assume(originalDataset.identifier === datasetOnForkedProject.identifier,
                 "Datasets on original project and fork have different identifiers"
          )

          datasetFinder.findDataset(originalDataset.identifier).unsafeRunSync() shouldBe
            originalDataset
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  originalDataset.project.to[DatasetProject],
                  datasetOnForkedProject.project.to[DatasetProject]
                ).sorted
              )
              .some
        }
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are sharing a dataset and one of the projects is forked" in new TestCase {
        val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne
        val dataset2Fork                = dataset2.forkProject().fork

        loadToStore(dataset1, dataset2, dataset2Fork)

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync() shouldBe
          dataset1
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                dataset1.project.to[DatasetProject],
                dataset2.project.to[DatasetProject],
                dataset2Fork.project.to[DatasetProject]
              ).sorted
            )
            .some

        assume(dataset2.identifier === dataset2Fork.identifier,
               "Datasets on original project and fork have different identifiers"
        )

        datasetFinder.findDataset(dataset2.identifier).unsafeRunSync() shouldBe
          dataset2
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                dataset1.project.to[DatasetProject],
                dataset2.project.to[DatasetProject],
                dataset2Fork.project.to[DatasetProject]
              ).sorted
            )
            .some
      }

    "return details of the dataset with the given id " +
      "- a case when a Renku created dataset is defined on a grandparent project with two levels of forks" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { grandparentDataset =>
          val parentDataset = grandparentDataset.forkProject().fork
          val childDataset  = parentDataset.forkProject().fork

          loadToStore(grandparentDataset, parentDataset, childDataset)

          assume(
            (grandparentDataset.identifier === parentDataset.identifier) && (parentDataset.identifier === childDataset.identifier),
            "Datasets on original project and fork have different identifiers"
          )

          datasetFinder.findDataset(grandparentDataset.identifier).unsafeRunSync() shouldBe
            grandparentDataset
              .to[NonModifiedDataset]
              .copy(
                usedIn = List(
                  grandparentDataset.project.to[DatasetProject],
                  parentDataset.project.to[DatasetProject],
                  childDataset.project.to[DatasetProject]
                ).sorted
              )
              .some
        }
      }

    "return details of the modified dataset with the given id " +
      "- case when modification is followed by forking" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { original =>
          val modified     = modifiedDatasetEntities(original).generateOne
          val modifiedFork = modified.forkProject().fork

          loadToStore(original, modified, modifiedFork)

          datasetFinder.findDataset(original.identifier).unsafeRunSync() shouldBe Some(
            original
              .to[NonModifiedDataset]
              .copy(usedIn = Nil)
          )

          datasetFinder.findDataset(modified.identifier).unsafeRunSync() shouldBe Some(
            modified
              .to[ModifiedDataset]
              .copy(
                usedIn = List(
                  modified.project.to[DatasetProject],
                  modifiedFork.project.to[DatasetProject]
                ).sorted
              )
          )
        }
      }

    "return details of the modified dataset with the given id " +
      "- case when modification is followed by forking and some other modification" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { original =>
          val modified      = modifiedDatasetEntities(original).generateOne
          val modifiedFork  = modified.forkProject().fork
          val modifiedAgain = modifiedDatasetEntities(modified).generateOne

          loadToStore(original, modified, modifiedFork, modifiedAgain)

          datasetFinder.findDataset(original.identifier).unsafeRunSync() shouldBe Some(
            original
              .to[NonModifiedDataset]
              .copy(usedIn = Nil)
          )

          assume(modified.identifier === modifiedFork.identifier, "Dataset after forking must have the same identifier")

          datasetFinder.findDataset(modified.identifier).unsafeRunSync() shouldBe Some(
            modified
              .to[ModifiedDataset]
              .copy(usedIn = List(modifiedFork.project.to[DatasetProject]))
          )

          datasetFinder.findDataset(modifiedAgain.identifier).unsafeRunSync() shouldBe Some(
            modifiedAgain.to[ModifiedDataset]
          )
        }
      }

    "return details of the dataset with the given id " +
      "- case when forking is followed by modification" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { original =>
          val originalForked = original.forkProject().fork
          val modifiedForked = modifiedDatasetEntities(originalForked).generateOne

          loadToStore(original, originalForked, modifiedForked)

          assume(original.identifier === originalForked.identifier,
                 "Dataset after forking must have the same identifier"
          )

          datasetFinder.findDataset(original.identifier).unsafeRunSync() shouldBe Some(
            original.to[NonModifiedDataset]
          )

          datasetFinder.findDataset(modifiedForked.identifier).unsafeRunSync() shouldBe Some(
            modifiedForked.to[ModifiedDataset]
          )
        }
      }

    "return details of the dataset with the given id " +
      "- case when a dataset on a fork is deleted" in new TestCase {
        forAll(datasetEntities(datasetProvenanceInternal)) { original =>
          val forked = original.forkProject().fork
          val invalidatedForked = forked
            .invalidate(invalidationTimes(forked.provenance.date).generateOne)
            .fold(errors => fail(errors.intercalate("; ")), identity)

          loadToStore(original, forked, invalidatedForked)

          datasetFinder.findDataset(original.identifier).unsafeRunSync() shouldBe Some(
            original.to[NonModifiedDataset]
          )

          datasetFinder.findDataset(invalidatedForked.identifier).unsafeRunSync() shouldBe None
        }
      }

    "return details of a fork dataset with the given id " +
      "- case when the parent of a fork dataset is deleted" in new TestCase {
        val originalDataset = datasetEntities(datasetProvenanceInternal).generateOne
        val datasetForked   = originalDataset.forkProject().fork
        val invalidatedOriginalDataset = originalDataset
          .invalidate(
            invalidationTimes(originalDataset.provenance.date).generateOne
          )
          .fold(errors => fail(errors.intercalate("; ")), identity)

        loadToStore(originalDataset, datasetForked, invalidatedOriginalDataset)

        datasetFinder.findDataset(datasetForked.identifier).unsafeRunSync() shouldBe Some(
          datasetForked.to[NonModifiedDataset]
        )

        datasetFinder.findDataset(invalidatedOriginalDataset.identifier).unsafeRunSync() shouldBe None
      }
  }

  "findDataset in the case of dataset import hierarchy" should {

    "return details of the dataset with the given id " +
      "- case when the first dataset is imported from a third party provider" in new TestCase {
        val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne
        val dataset3                    = dataset2.importTo(projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne)

        loadToStore(dataset1, dataset2, dataset3)

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync() shouldBe Some(
          dataset1
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                dataset1.project.to[DatasetProject],
                dataset2.project.to[DatasetProject],
                dataset3.project.to[DatasetProject]
              ).sorted
            )
        )

        datasetFinder.findDataset(dataset3.identifier).unsafeRunSync() shouldBe Some(
          dataset3
            .to[NonModifiedDataset]
            .copy(
              sameAs = dataset2.provenance.sameAs,
              usedIn = List(
                dataset1.project.to[DatasetProject],
                dataset2.project.to[DatasetProject],
                dataset3.project.to[DatasetProject]
              ).sorted
            )
        )
      }

    "return details of the dataset with the given id " +
      "- case when the first dataset is renku created" in new TestCase {

        val dataset1 = datasetEntities(datasetProvenanceInternal).generateOne
        val dataset2 = dataset1.importTo(projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne)
        val dataset3 = dataset2.importTo(projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne)

        loadToStore(dataset1, dataset2, dataset3)

        datasetFinder
          .findDataset(dataset1.identifier)
          .unsafeRunSync() shouldBe Some(
          dataset1
            .to[NonModifiedDataset]
            .copy(
              usedIn = List(
                dataset1.project.to[DatasetProject],
                dataset2.project.to[DatasetProject],
                dataset3.project.to[DatasetProject]
              ).sorted
            )
        )

        datasetFinder
          .findDataset(dataset3.identifier)
          .unsafeRunSync() shouldBe Some(
          dataset3
            .to[NonModifiedDataset]
            .copy(
              sameAs = SameAs(dataset1.provenance.entityId),
              usedIn = List(
                dataset1.project.to[DatasetProject],
                dataset2.project.to[DatasetProject],
                dataset3.project.to[DatasetProject]
              ).sorted
            )
        )
      }

    "return details of the dataset with the given id " +
      "- case when the sameAs hierarchy is broken by dataset modification" in new TestCase {
        val dataset1         = datasetEntities(datasetProvenanceInternal).generateOne
        val dataset2         = dataset1.importTo(projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne)
        val dataset2Modified = modifiedDatasetEntities(dataset2).generateOne
        val dataset3         = dataset2Modified.importTo(projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne)

        loadToStore(dataset1, dataset2, dataset2Modified, dataset3)

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync() shouldBe Some(dataset1.to[NonModifiedDataset])
        datasetFinder.findDataset(dataset2Modified.identifier).unsafeRunSync() shouldBe Some(
          dataset2Modified
            .to[ModifiedDataset]
            .copy(
              usedIn = List(
                dataset2Modified.project.to[DatasetProject],
                dataset3.project.to[DatasetProject]
              ).sorted
            )
        )
      }

    "not return the details of a dataset" +
      "- case when the latest import of the dataset has been invalidated" in new TestCase {

        val dataset1 = datasetEntities(datasetProvenanceInternal).generateOne
        val dataset2 = dataset1.importTo(projectEntities[ForksCount.Zero](visibilityPublic).generateOne)
        val invalidated = dataset2
          .invalidate(
            invalidationTimes(dataset2.provenance.date.instant, dataset2.project.dateCreated.value).generateOne
          )
          .fold(errors => fail(errors.intercalate("; ")), identity)

        loadToStore(dataset1, dataset2, invalidated)

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync()    shouldBe Some(dataset1.to[NonModifiedDataset])
        datasetFinder.findDataset(dataset2.identifier).unsafeRunSync()    shouldBe None
        datasetFinder.findDataset(invalidated.identifier).unsafeRunSync() shouldBe None
      }

    "not return the details of a dataset" +
      "- case when the original dataset has been invalidated" in new TestCase {

        val dataset1 = datasetEntities(datasetProvenanceInternal).generateOne
        val dataset2 = dataset1.importTo(projectEntities[ForksCount.Zero](visibilityPublic).generateOne)
        val invalidated = dataset1
          .invalidate(invalidationTimes(dataset1.provenance.date).generateOne)
          .fold(errors => fail(errors.intercalate("; ")), identity)

        loadToStore(dataset1, invalidated, dataset2)

        datasetFinder.findDataset(dataset1.identifier).unsafeRunSync()    shouldBe None
        datasetFinder.findDataset(invalidated.identifier).unsafeRunSync() shouldBe None
        datasetFinder.findDataset(dataset2.identifier).unsafeRunSync()    shouldBe Some(dataset2.to[NonModifiedDataset])
      }

    "not return the details of a dataset" +
      "- case when the latest modification of the dataset has been invalidated" in new TestCase {

        val dataset         = datasetEntities(datasetProvenanceInternal).generateOne
        val datasetModified = modifiedDatasetEntities(dataset).generateOne
        val datasetModifiedInvalidated = datasetModified
          .invalidate(invalidationTimes(datasetModified.provenance.date).generateOne)
          .fold(errors => fail(errors.intercalate("; ")), identity)

        loadToStore(dataset, datasetModified, datasetModifiedInvalidated)

        datasetFinder.findDataset(dataset.identifier).unsafeRunSync() shouldBe Some(
          dataset.to[NonModifiedDataset].copy(usedIn = Nil)
        )
        datasetFinder.findDataset(datasetModified.identifier).unsafeRunSync()            shouldBe None
        datasetFinder.findDataset(datasetModifiedInvalidated.identifier).unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {
    implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetFinder = new DatasetFinderImpl(
      new BaseDetailsFinder(rdfStoreConfig, logger, timeRecorder),
      new CreatorsFinder(rdfStoreConfig, logger, timeRecorder),
      new PartsFinder(rdfStoreConfig, logger, timeRecorder),
      new ProjectsFinder(rdfStoreConfig, logger, timeRecorder)
    )
  }

  private implicit lazy val usedInOrdering: Ordering[DatasetProject] = Ordering.by(_.name)
}
