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
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DateCreatedInProject, InitialVersion, TopmostSameAs}
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.EntityGenerators.invalidationEntity
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.EntitiesGenerators.projectEntities
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.language.postfixOps

class IODatasetFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "findDataset" should {
    "return details of the dataset with the given id " +
      "- a case of a single used in-project created dataset" in new TestCase {
        forAll(datasetProjects, addedToProjectObjects) { (project, addedToProject) =>
          val dataset = nonModifiedDatasets(projects = project.copy(created = addedToProject).toGenerator).generateOne

          loadToStore(
            dataset.toJsonLD(noSameAs = true)(),
            randomDataSetCommit
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              sameAs = dataset.entityId.asSameAs,
              parts = dataset.parts.sorted,
              projects = List(DatasetProject(project.path, project.name, addedToProject)),
              keywords = dataset.keywords.sorted
            )
          )
        }
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same imported dataset" in new TestCase {
        forAll(datasetProjects, addedToProjectObjects, datasetProjects, addedToProjectObjects) {
          (project1, addedToProject1, project2, addedToProject2) =>
            val sameAs = datasetSameAs.generateOne
            val dataset1 = nonModifiedDatasets(
              Gen.const(sameAs),
              projects = project1.copy(created = addedToProject1).toGenerator
            ).generateOne

            val dataset2 = nonModifiedDatasets(
              Gen.const(sameAs),
              projects = project2.copy(created = addedToProject2).toGenerator
            ).generateOne

            loadToStore(
              dataset1.toJsonLD()(topmostSameAs = TopmostSameAs(sameAs)),
              dataset2.toJsonLD()(topmostSameAs = TopmostSameAs(sameAs)),
              randomDataSetCommit
            )

            datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
              dataset1.copy(
                parts = dataset1.parts.sorted,
                projects = List(DatasetProject(project1.path, project1.name, addedToProject1),
                                DatasetProject(project2.path, project2.name, addedToProject2)
                ).sorted,
                keywords = dataset1.keywords.sorted
              )
            )

            datasetFinder.findDataset(dataset2.id).unsafeRunSync() shouldBe Some(
              dataset2.copy(
                parts = dataset2.parts.sorted,
                projects = List(DatasetProject(project1.path, project1.name, addedToProject1),
                                DatasetProject(project2.path, project2.name, addedToProject2)
                ).sorted,
                keywords = dataset2.keywords.sorted
              )
            )
        }
      }

    "return details of the dataset with the given id " +
      "- a case when dataset is modified" in new TestCase {
        forAll(datasetProjects, addedToProjectObjects) { (project, addedToProject) =>
          val dataset = nonModifiedDatasets(projects = project.copy(created = addedToProject).toGenerator).generateOne
          val modifiedOnProject = addedToProjectObjects.generateOne.copy(
            date = DateCreatedInProject(
              timestampsNotInTheFuture(butOlderThan = addedToProject.date.value).generateOne
            )
          )
          val modifiedDataset =
            modifiedDatasetsOnFirstProject(dataset.changeCreationOnProject(to = modifiedOnProject)).generateOne.copy(
              maybeDescription = datasetDescriptions.generateSome
            )

          loadToStore(
            dataset.toJsonLD()(),
            modifiedDataset.toJsonLD(),
            randomDataSetCommit
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              parts = dataset.parts.sorted,
              projects = List(DatasetProject(project.path, project.name, addedToProject)).sorted,
              keywords = dataset.keywords.sorted
            )
          )
          datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe Some(
            modifiedDataset.copy(
              parts = modifiedDataset.parts.sorted,
              projects = List(DatasetProject(project.path, project.name, modifiedOnProject)).sorted,
              keywords = modifiedDataset.keywords.sorted
            )
          )
        }
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset created in a Renku project" in new TestCase {
        forAll(datasetProjects,
               addedToProjectObjects,
               datasetProjects,
               addedToProjectObjects,
               datasetProjects,
               addedToProjectObjects
        ) { (project1, addedToProject1, project2, addedToProject2, project3, addedToProject3) =>
          val sourceDataset = nonModifiedDatasets(
            projects = project1.copy(created = addedToProject1).toGenerator
          ).generateOne
          val dataset2Id = datasetIdentifiers.generateOne
          val dataset2 = sourceDataset.copy(
            id = dataset2Id,
            sameAs = sourceDataset.entityId.asSameAs,
            versions = DatasetVersions(InitialVersion(dataset2Id)),
            projects = List(DatasetProject(project2.path, project2.name, addedToProject2))
          )
          val dataset3Id = datasetIdentifiers.generateOne
          val dataset3 = sourceDataset.copy(
            id = dataset3Id,
            sameAs = sourceDataset.entityId.asSameAs,
            versions = DatasetVersions(InitialVersion(dataset3Id)),
            projects = List(DatasetProject(project3.path, project3.name, addedToProject3))
          ) // to simulate adding the first project's original dataset to another project

          loadToStore(
            sourceDataset.toJsonLD(noSameAs = true)(topmostSameAs = sourceDataset.entityId.asTopmostSameAs),
            dataset2.toJsonLD()(topmostSameAs = sourceDataset.entityId.asTopmostSameAs),
            dataset3.toJsonLD()(topmostSameAs = sourceDataset.entityId.asTopmostSameAs),
            randomDataSetCommit
          )

          datasetFinder.findDataset(sourceDataset.id).unsafeRunSync() shouldBe Some(
            sourceDataset.copy(
              sameAs = sourceDataset.entityId.asSameAs,
              parts = sourceDataset.parts.sorted,
              projects = List(
                DatasetProject(project1.path, project1.name, addedToProject1),
                DatasetProject(project2.path, project2.name, addedToProject2),
                DatasetProject(project3.path, project3.name, addedToProject3)
              ).sorted,
              keywords = sourceDataset.keywords.sorted
            )
          )

          datasetFinder.findDataset(dataset2.id).unsafeRunSync() shouldBe Some(
            dataset2.copy(
              sameAs = sourceDataset.entityId.asSameAs,
              parts = dataset2.parts.sorted,
              projects = List(
                DatasetProject(project1.path, project1.name, addedToProject1),
                DatasetProject(project2.path, project2.name, addedToProject2),
                DatasetProject(project3.path, project3.name, addedToProject3)
              ).sorted,
              keywords = dataset2.keywords.sorted
            )
          )
        }
      }

    "return None if there are no datasets with the given id" in new TestCase {
      val identifier = datasetIdentifiers.generateOne
      datasetFinder.findDataset(identifier).unsafeRunSync() shouldBe None
    }
  }

  "findDataset in case of forks" should {

    "return details of the dataset with the given id " +
      "- a case when an in-project created dataset is defined on a project which has a fork" in new TestCase {
        forAll(datasetProjects, datasetProjects, addedToProjectObjects, commitIds) {
          (sourceProject, forkProject, addedToProject, commitId) =>
            val dataset = nonModifiedDatasets(
              projects = sourceProject.copy(created = addedToProject).toGenerator
            ).generateOne

            loadToStore(
              dataset.toJsonLD(noSameAs = true, commitId = commitId)(),
              dataset // to simulate forking the sourceProject
                .copy(projects = List(DatasetProject(forkProject.path, forkProject.name, addedToProject)))
                .toJsonLD(noSameAs = true, commitId = commitId)()
            )

            datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
              dataset.copy(
                sameAs = dataset.entityId.asSameAs,
                parts = dataset.parts.sorted,
                projects = List(
                  DatasetProject(sourceProject.path, sourceProject.name, addedToProject),
                  DatasetProject(forkProject.path, forkProject.name, addedToProject)
                ).sorted,
                keywords = dataset.keywords.sorted
              )
            )
        }
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are sharing a dataset and one of the projects is forked" in new TestCase {
        forAll(datasetProjects, addedToProjectObjects, datasetProjects, addedToProjectObjects, datasetProjects) {
          (project1, addedToProject1, project2, addedToProject2, project2Fork) =>
            val dataset = nonModifiedDatasets(
              projects = project1.copy(created = addedToProject1).toGenerator
            ).generateOne
            val project2DatasetCommit = commitIds.generateOne

            // to simulate adding the same data-set to another project
            val importedDatasetId = datasetIdentifiers.generateOne
            val importedDataset = dataset.copy(
              id = importedDatasetId,
              versions = DatasetVersions(InitialVersion(importedDatasetId)),
              projects = List(DatasetProject(project2.path, project2.name, addedToProject2))
            )

            val forkedDataset = importedDataset.copy(
              projects = List(DatasetProject(project2Fork.path, project2Fork.name, addedToProject2))
            )

            loadToStore(
              dataset.toJsonLD()(topmostSameAs = TopmostSameAs(dataset.sameAs)),
              importedDataset.toJsonLD(commitId = project2DatasetCommit)(topmostSameAs = TopmostSameAs(dataset.sameAs)),
              forkedDataset.toJsonLD(commitId = project2DatasetCommit)(topmostSameAs = TopmostSameAs(dataset.sameAs))
            )

            datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
              dataset.copy(
                parts = dataset.parts.sorted,
                projects = List(
                  DatasetProject(project1.path, project1.name, addedToProject1),
                  DatasetProject(project2.path, project2.name, addedToProject2),
                  DatasetProject(project2Fork.path, project2Fork.name, addedToProject2)
                ).sorted,
                keywords = dataset.keywords.sorted
              )
            )

            datasetFinder.findDataset(forkedDataset.id).unsafeRunSync() shouldBe Some(
              forkedDataset.copy(
                parts = dataset.parts.sorted,
                projects = List(
                  DatasetProject(project1.path, project1.name, addedToProject1),
                  DatasetProject(project2.path, project2.name, addedToProject2),
                  DatasetProject(project2Fork.path, project2Fork.name, addedToProject2)
                ).sorted,
                keywords = dataset.keywords.sorted
              )
            )
        }
      }

    "return details of the dataset with the given id " +
      "- a case when an in-project created dataset is defined on a grandparent project with two levels of forks" in new TestCase {
        forAll(datasetProjects, datasetProjects, datasetProjects, addedToProjectObjects, commitIds) {
          (grandparentProject, parentProject, childProject, addedToProject, commitId) =>
            val datasetOnGrandparent = nonModifiedDatasets(
              projects = grandparentProject.copy(created = addedToProject).toGenerator
            ).generateOne

            loadToStore(
              datasetOnGrandparent.toJsonLD(noSameAs = true, commitId = commitId)(),
              datasetOnGrandparent // to simulate forking the grandparentProject - dataset on a parent project
                .copy(projects = List(DatasetProject(parentProject.path, parentProject.name, addedToProject)))
                .toJsonLD(noSameAs = true, commitId = commitId)(),
              datasetOnGrandparent // to simulate forking the parentProject - dataset on a child project
                .copy(projects = List(DatasetProject(childProject.path, childProject.name, addedToProject)))
                .toJsonLD(noSameAs = true, commitId = commitId)()
            )

            datasetFinder.findDataset(datasetOnGrandparent.id).unsafeRunSync() shouldBe Some(
              datasetOnGrandparent.copy(
                sameAs = datasetOnGrandparent.entityId.asSameAs,
                parts = datasetOnGrandparent.parts.sorted,
                projects = List(
                  DatasetProject(grandparentProject.path, grandparentProject.name, addedToProject),
                  DatasetProject(parentProject.path, parentProject.name, addedToProject),
                  DatasetProject(childProject.path, childProject.name, addedToProject)
                ).sorted,
                keywords = datasetOnGrandparent.keywords.sorted
              )
            )
        }
      }

    "return details of the dataset with the given id " +
      "- case when the forking hierarchy is broken by dataset modification" in new TestCase {
        forAll(datasetProjects, addedToProjectObjects, datasetProjects) { (project, addedToProject, forkedProject) =>
          val dataset = nonModifiedDatasets(
            projects = project.copy(created = addedToProject).toGenerator
          ).generateOne
          val projectDatasetModificationCommit = commitIds.generateOne
          val modifiedOnProject = addedToProjectObjects.generateOne.copy(
            date = DateCreatedInProject(
              timestampsNotInTheFuture(butOlderThan = addedToProject.date.value).generateOne
            )
          )
          val modifiedDataset = modifiedDatasetsOnFirstProject(
            dataset.changeCreationOnProject(to = modifiedOnProject)
          ).generateOne.copy(maybeDescription = datasetDescriptions.generateSome)

          loadToStore(
            dataset.toJsonLD()(topmostSameAs = TopmostSameAs(dataset.sameAs)),
            modifiedDataset.toJsonLD(
              commitId = projectDatasetModificationCommit,
              topmostDerivedFrom = dataset.entityId.asTopmostDerivedFrom
            ), // to simulate modifying the data-set
            modifiedDataset // to simulate forking project after dataset modification
              .copy(projects = List(DatasetProject(forkedProject.path, forkedProject.name, modifiedOnProject)))
              .toJsonLD(commitId = projectDatasetModificationCommit,
                        topmostDerivedFrom = dataset.entityId.asTopmostDerivedFrom
              )
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              parts = dataset.parts.sorted,
              projects = List(DatasetProject(project.path, project.name, addedToProject)).sorted,
              keywords = dataset.keywords.sorted
            )
          )
          datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe Some(
            modifiedDataset.copy(
              parts = modifiedDataset.parts.sorted,
              projects = List(
                DatasetProject(project.path, project.name, modifiedOnProject),
                DatasetProject(forkedProject.path, forkedProject.name, modifiedOnProject)
              ).sorted,
              keywords = modifiedDataset.keywords.sorted
            )
          )
        }
      }
  }

  "findDataset in the case of dataset import hierarchy" should {

    "return details of the dataset with the given id " +
      "- case when the first dataset is imported from a third party provider" in new TestCase {
        val dataset1Project = datasetProjects.generateOne
        val sharedSameAs    = datasetSameAs.generateOne
        val dataset1 = nonModifiedDatasets().generateOne.copy(
          projects = List(dataset1Project),
          sameAs = sharedSameAs
        )

        val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
        val dataset2 = dataset1.copy(
          id = datasetIdentifiers.generateOne,
          sameAs = dataset1.entityId.asSameAs,
          projects = List(dataset2Project)
        )

        val dataset2ModifiedOldWay = dataset2.copy(
          projects = List(dataset2Project shiftDateAfter dataset2Project)
        )

        val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
        val dataset3 = dataset2.copy(
          id = datasetIdentifiers.generateOne,
          sameAs = dataset2.entityId.asSameAs,
          projects = List(dataset3Project)
        )

        loadToStore(
          dataset1.toJsonLD()(topmostSameAs = TopmostSameAs(sharedSameAs)),
          dataset2.toJsonLD()(topmostSameAs = TopmostSameAs(sharedSameAs)),
          dataset2ModifiedOldWay.toJsonLD()(topmostSameAs = TopmostSameAs(sharedSameAs)),
          dataset3.toJsonLD()(topmostSameAs = TopmostSameAs(sharedSameAs))
        )

        datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
          dataset1.copy(
            parts = dataset1.parts.sorted,
            projects = List(
              dataset1Project,
              dataset2Project,
              dataset3Project
            ).sorted,
            keywords = dataset1.keywords.sorted
          )
        )
      }

    "return details of the dataset with the given id - case when the first dataset is in-project created" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = nonModifiedDatasets().generateOne
        .copy(projects = List(dataset1Project))

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id = datasetIdentifiers.generateOne,
        sameAs = dataset1.entityId.asSameAs,
        projects = List(dataset2Project)
      )

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id = datasetIdentifiers.generateOne,
        sameAs = dataset2.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(
        dataset1.toJsonLD(noSameAs = true)(topmostSameAs = dataset1.entityId.asTopmostSameAs),
        dataset2.toJsonLD()(topmostSameAs = dataset1.entityId.asTopmostSameAs),
        dataset3.toJsonLD()(topmostSameAs = dataset1.entityId.asTopmostSameAs)
      )

      datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
        dataset1.copy(
          sameAs = dataset1.entityId.asSameAs,
          parts = dataset1.parts.sorted,
          projects = List(
            dataset1Project,
            dataset2Project,
            dataset3Project
          ).sorted,
          keywords = dataset1.keywords.sorted
        )
      )
    }

    "return details of the dataset with the given id - " +
      "case when the requested id is anywhere in the hierarchy" in new TestCase {
        val dataset1Project = datasetProjects.generateOne
        val dataset1 = nonModifiedDatasets().generateOne
          .copy(projects = List(dataset1Project))

        val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
        val dataset2Id      = datasetIdentifiers.generateOne
        val dataset2 = dataset1.copy(
          id = dataset2Id,
          sameAs = dataset1.entityId.asSameAs,
          versions = DatasetVersions(InitialVersion(dataset2Id)),
          projects = List(dataset2Project)
        )

        val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
        val dataset3Id      = datasetIdentifiers.generateOne
        val dataset3 = dataset2.copy(
          id = dataset3Id,
          sameAs = dataset2.entityId.asSameAs,
          versions = DatasetVersions(InitialVersion(dataset3Id.toString)),
          projects = List(dataset3Project)
        )

        loadToStore(
          dataset1.toJsonLD(noSameAs = true)(topmostSameAs = dataset1.entityId.asTopmostSameAs),
          dataset2.toJsonLD()(topmostSameAs = dataset1.entityId.asTopmostSameAs),
          dataset3.toJsonLD()(topmostSameAs = dataset1.entityId.asTopmostSameAs)
        )

        datasetFinder.findDataset(dataset2.id).unsafeRunSync() shouldBe Some(
          dataset2.copy(
            sameAs = dataset1.entityId.asSameAs,
            parts = dataset2.parts.sorted,
            projects = List(
              dataset1Project,
              dataset2Project,
              dataset3Project
            ).sorted,
            keywords = dataset2.keywords.sorted
          )
        )

        datasetFinder.findDataset(dataset3.id).unsafeRunSync() shouldBe Some(
          dataset3.copy(
            sameAs = dataset1.entityId.asSameAs,
            parts = dataset3.parts.sorted,
            projects = List(
              dataset1Project,
              dataset2Project,
              dataset3Project
            ).sorted,
            keywords = dataset3.keywords.sorted
          )
        )
      }

    "return details of the dataset with the given id " +
      "- case when the sameAs hierarchy is broken by dataset modification" in new TestCase {
        val dataset1Project = datasetProjects.generateOne
        val dataset1 = nonModifiedDatasets().generateOne.copy(
          projects = List(dataset1Project)
        )

        val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
        val dataset2Id      = datasetIdentifiers.generateOne
        val dataset2 = dataset1.copy(
          id = dataset2Id,
          sameAs = dataset1.entityId.asSameAs,
          versions = DatasetVersions(InitialVersion(dataset2Id)),
          projects = List(dataset2Project)
        )

        val modifiedOnProject2 = addedToProjectObjects.generateOne.copy(
          date = DateCreatedInProject(
            timestampsNotInTheFuture(butOlderThan = dataset2Project.created.date.value).generateOne
          )
        )
        val dataset2ModifiedProject = dataset2Project.copy(created = modifiedOnProject2)
        val modifiedDataset2 = modifiedDatasetsOnFirstProject(
          dataset2.copy(projects = List(dataset2ModifiedProject))
        ).generateOne.copy(maybeDescription = datasetDescriptions.generateSome)

        val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project.copy(
          created = modifiedOnProject2
        )
        val dataset3Id = datasetIdentifiers.generateOne
        val dataset3 = NonModifiedDataset(
          id = dataset3Id,
          title = modifiedDataset2.title,
          name = modifiedDataset2.name,
          url = datasetUrls.generateOne,
          sameAs = modifiedDataset2.entityId.asSameAs,
          versions = DatasetVersions(InitialVersion(dataset3Id)),
          maybeDescription = datasetDescriptions.generateSome,
          published = modifiedDataset2.published,
          created = modifiedDataset2.created,
          parts = modifiedDataset2.parts,
          projects = List(dataset3Project),
          keywords = modifiedDataset2.keywords,
          images = modifiedDataset2.images
        )

        loadToStore(
          dataset1.toJsonLD()(topmostSameAs = dataset1.entityId.asTopmostSameAs),
          dataset2.toJsonLD()(topmostSameAs = dataset1.entityId.asTopmostSameAs),
          modifiedDataset2.toJsonLD(topmostDerivedFrom = dataset2.entityId.asTopmostDerivedFrom),
          dataset3.toJsonLD()(topmostSameAs = modifiedDataset2.entityId.asTopmostSameAs)
        )

        datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
          dataset1.copy(
            parts = dataset1.parts.sorted,
            sameAs = dataset1.entityId.asSameAs,
            projects = List(dataset1Project, dataset2Project).sorted,
            keywords = dataset1.keywords.sorted
          )
        )
        datasetFinder.findDataset(modifiedDataset2.id).unsafeRunSync() shouldBe Some(
          modifiedDataset2.copy(
            parts = modifiedDataset2.parts.sorted,
            projects = List(dataset2ModifiedProject, dataset3Project).sorted,
            keywords = modifiedDataset2.keywords.sorted
          )
        )
      }

    "not return the details of a dataset" +
      "- case when the dataset has been invalidated" in new TestCase {
        val project = projectEntities.generateOne
        val dataset = nonModifiedDatasets().generateOne.copy(
          projects = List(project.toDatasetProject)
        )

        val entityWithInvalidation = invalidationEntity(dataset.id, project).generateOne

        loadToStore(
          dataset.toJsonLD()(),
          entityWithInvalidation.asJsonLD
        )

        datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe None
      }

    "not return the details of a dataset" +
      "- case when the latest version of the dataset has been invalidated" in new TestCase {
        val project = projectEntities.generateOne
        private val datasetProject: DatasetProject = project.toDatasetProject
        val dataset = nonModifiedDatasets().generateOne.copy(
          projects = List(datasetProject)
        )

        val modifiedDataset = modifiedDatasetsOnFirstProject(
          dataset.copy(projects = List(datasetProject))
        ).generateOne.copy(maybeDescription = datasetDescriptions.generateSome)

        val entityWithInvalidation =
          invalidationEntity(modifiedDataset.id, project, dataset.entityId.asTopmostDerivedFrom.some).generateOne

        loadToStore(
          dataset.toJsonLD()(),
          modifiedDataset.toJsonLD(topmostDerivedFrom = dataset.entityId.asTopmostDerivedFrom),
          entityWithInvalidation.asJsonLD
        )

        datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe None
      }

    "not return the details of a dataset" +
      "- case when the latest version of the dataset has been invalidated " +
      "and the requested id is anywhere in the hierarchy" in new TestCase {
        val project = projectEntities.generateOne
        private val datasetProject: DatasetProject = project.toDatasetProject
        val dataset = nonModifiedDatasets().generateOne.copy(
          projects = List(datasetProject)
        )

        val modifiedDataset = modifiedDatasetsOnFirstProject(
          dataset.copy(projects = List(datasetProject))
        ).generateOne.copy(maybeDescription = datasetDescriptions.generateSome)

        val modifiedDataset2 = modifiedDatasetsOnFirstProject(
          modifiedDataset.copy(projects = List(datasetProject))
        ).generateOne.copy(maybeDescription = datasetDescriptions.generateSome)

        val entityWithInvalidation =
          invalidationEntity(modifiedDataset2.id, project, dataset.entityId.asTopmostDerivedFrom.some).generateOne

        loadToStore(
          dataset.toJsonLD()(),
          modifiedDataset.toJsonLD(topmostDerivedFrom = dataset.entityId.asTopmostDerivedFrom),
          modifiedDataset2.toJsonLD(topmostDerivedFrom = dataset.entityId.asTopmostDerivedFrom),
          entityWithInvalidation.asJsonLD
        )

        datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetFinder = new IODatasetFinder(
      new BaseDetailsFinder(rdfStoreConfig, logger, timeRecorder),
      new CreatorsFinder(rdfStoreConfig, logger, timeRecorder),
      new PartsFinder(rdfStoreConfig, logger, timeRecorder),
      new ProjectsFinder(rdfStoreConfig, logger, timeRecorder)
    )
  }

  private implicit lazy val partsAlphabeticalOrdering: Ordering[DatasetPart] =
    (part1: DatasetPart, part2: DatasetPart) => part1.name compareTo part2.name

  private implicit lazy val projectsAlphabeticalOrdering: Ordering[DatasetProject] =
    (project1: DatasetProject, project2: DatasetProject) => project1.name compareTo project2.name
}
