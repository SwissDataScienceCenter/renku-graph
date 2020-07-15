/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DateCreated, DateCreatedInProject, SameAs}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.entities.{DataSet, Person}
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.{EntityId, JsonLD}
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetFinderSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "findDataset" should {

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same imported dataset" in new TestCase {
      forAll(datasetProjects, addedToProjectObjects, datasetProjects, addedToProjectObjects) {
        (project1, addedToProject1, project2, addedToProject2) =>
          val dataset = nonModifiedDatasets(projects = project1.copy(created = addedToProject1).toGenerator).generateOne
          val modifiedOnProject1 = addedToProjectObjects.generateOne.copy(
            date = datasetInProjectCreationDates generateGreaterThan addedToProject1.date
          )
          val modifiedDataset = modifiedDatasets(dataset, project1.copy(created = modifiedOnProject1)).generateOne.copy(
            maybeDescription = datasetDescriptions.generateSome
          )

          loadToStore(
            dataset.toJsonLD(),
            modifiedDataset.toJsonLD(), // simulating dataset modification
            dataset // to simulate adding the same data-set to another project
              .copy(id       = datasetIdentifiers.generateOne,
                    sameAs   = dataset.entityId.asSameAs,
                    projects = List(DatasetProject(project2.path, project2.name, addedToProject2)))
              .toJsonLD(),
            randomDataSetCommit
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              parts = dataset.parts.sorted,
              projects = List(DatasetProject(project1.path, project1.name, addedToProject1),
                              DatasetProject(project2.path, project2.name, addedToProject2)).sorted
            )
          )
          datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe Some(
            modifiedDataset.copy(
              parts    = modifiedDataset.parts.sorted,
              projects = List(DatasetProject(project1.path, project1.name, modifiedOnProject1)).sorted
            )
          )
      }
    }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset created in a Renku project" in new TestCase {
      forAll(datasetProjects, addedToProjectObjects, datasetProjects, addedToProjectObjects) {
        (project1, addedToProject1, project2, addedToProject2) =>
          val dataset = nonModifiedDatasets(projects = project1.copy(created = addedToProject1).toGenerator).generateOne
          val modifiedOnProject1 = addedToProjectObjects.generateOne.copy(
            date = datasetInProjectCreationDates generateGreaterThan addedToProject1.date
          )
          val modifiedDataset = modifiedDatasets(dataset, project1.copy(created = modifiedOnProject1)).generateOne.copy(
            maybeDescription = datasetDescriptions.generateSome
          )

          loadToStore(
            dataset.toJsonLD(noSameAs = true),
            modifiedDataset.toJsonLD(), // simulating dataset modification
            dataset // to simulate adding the first project's original data-set to another project
              .copy(id       = datasetIdentifiers.generateOne,
                    sameAs   = dataset.entityId.asSameAs,
                    projects = List(DatasetProject(project2.path, project2.name, addedToProject2)))
              .toJsonLD(),
            randomDataSetCommit
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              sameAs = dataset.entityId.asSameAs,
              parts  = dataset.parts.sorted,
              projects = List(DatasetProject(project1.path, project1.name, addedToProject1),
                              DatasetProject(project2.path, project2.name, addedToProject2)).sorted
            )
          )
          datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe Some(
            modifiedDataset.copy(
              parts    = modifiedDataset.parts.sorted,
              projects = List(DatasetProject(project1.path, project1.name, modifiedOnProject1)).sorted
            )
          )
      }
    }

    "return details of the dataset with the given id " +
      "- a case when an imported dataset is used in one project" in new TestCase {
      forAll(datasetProjects, addedToProjectObjects) { (project, addedToProject) =>
        val dataset = nonModifiedDatasets(projects = project.copy(created = addedToProject).toGenerator).generateOne
        val modifiedOnProject = addedToProjectObjects.generateOne.copy(
          date = datasetInProjectCreationDates generateGreaterThan addedToProject.date
        )
        val modifiedDataset = modifiedDatasets(dataset, project.copy(created = modifiedOnProject)).generateOne.copy(
          maybeDescription = datasetDescriptions.generateSome
        )

        loadToStore(
          dataset.toJsonLD(),
          modifiedDataset.toJsonLD(), // simulating dataset modification
          randomDataSetCommit
        )

        datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
          dataset.copy(
            parts    = dataset.parts.sorted,
            projects = List(DatasetProject(project.path, project.name, addedToProject))
          )
        )
        datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe Some(
          modifiedDataset.copy(
            parts    = modifiedDataset.parts.sorted,
            projects = List(DatasetProject(project.path, project.name, modifiedOnProject)).sorted
          )
        )
      }
    }

    "return details of the dataset with the given id " +
      "- a case of a single used in-project created dataset" in new TestCase {
      forAll(datasetProjects, addedToProjectObjects) { (project, addedToProject) =>
        val dataset = nonModifiedDatasets(projects = project.copy(created = addedToProject).toGenerator).generateOne
        val modifiedOnProject = addedToProjectObjects.generateOne.copy(
          date = datasetInProjectCreationDates generateGreaterThan addedToProject.date
        )
        val modifiedDataset = modifiedDatasets(dataset, project.copy(created = modifiedOnProject)).generateOne.copy(
          maybeDescription = datasetDescriptions.generateSome
        )

        loadToStore(
          dataset.toJsonLD(noSameAs = true),
          modifiedDataset.toJsonLD(), // simulating dataset modification
          randomDataSetCommit
        )

        datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
          dataset.copy(
            sameAs   = dataset.entityId.asSameAs,
            parts    = dataset.parts.sorted,
            projects = List(DatasetProject(project.path, project.name, addedToProject))
          )
        )
        datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe Some(
          modifiedDataset.copy(
            parts    = modifiedDataset.parts.sorted,
            projects = List(DatasetProject(project.path, project.name, modifiedOnProject)).sorted
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
      "- a case when unrelated projects are using the same imported dataset and one of the projects is forked" in new TestCase {
      forAll(datasetProjects, addedToProjectObjects, datasetProjects, addedToProjectObjects, datasetProjects) {
        (project1, addedToProject1, project2, addedToProject2, project2Fork) =>
          val dataset               = nonModifiedDatasets(projects = project1.copy(created = addedToProject1).toGenerator).generateOne
          val project2DatasetCommit = commitIds.generateOne
          val project2DatasetId     = datasetIdentifiers.generateOne

          loadToStore(
            dataset.toJsonLD(),
            dataset // to simulate adding the same data-set to another project
              .copy(id       = project2DatasetId,
                    projects = List(DatasetProject(project2.path, project2.name, addedToProject2)))
              .toJsonLD(commitId = project2DatasetCommit),
            dataset // to simulate forking project2
              .copy(id       = project2DatasetId,
                    projects = List(DatasetProject(project2Fork.path, project2Fork.name, addedToProject2)))
              .toJsonLD(commitId = project2DatasetCommit)
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              parts = dataset.parts.sorted,
              projects = List(
                DatasetProject(project1.path, project1.name, addedToProject1),
                DatasetProject(project2.path, project2.name, addedToProject2),
                DatasetProject(project2Fork.path, project2Fork.name, addedToProject2)
              ).sorted
            )
          )
      }
    }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset created in renku project and one of the projects is forked" in new TestCase {
      forAll(datasetProjects, addedToProjectObjects, datasetProjects, addedToProjectObjects, datasetProjects) {
        (project1, addedToProject1, project2, addedToProject2, project2Fork) =>
          val dataset               = nonModifiedDatasets(projects = project1.copy(created = addedToProject1).toGenerator).generateOne
          val project2DatasetCommit = commitIds.generateOne
          val project2DatasetId     = datasetIdentifiers.generateOne

          loadToStore(
            dataset.toJsonLD(noSameAs = true),
            dataset // to simulate adding first project's data-set to another project
              .copy(id       = project2DatasetId,
                    sameAs   = dataset.entityId.asSameAs,
                    projects = List(DatasetProject(project2.path, project2.name, addedToProject2)))
              .toJsonLD(commitId = project2DatasetCommit),
            dataset // to simulate forking project2
              .copy(id       = project2DatasetId,
                    sameAs   = dataset.entityId.asSameAs,
                    projects = List(DatasetProject(project2Fork.path, project2Fork.name, addedToProject2)))
              .toJsonLD(commitId = project2DatasetCommit)
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              sameAs = dataset.entityId.asSameAs,
              parts  = dataset.parts.sorted,
              projects = List(
                DatasetProject(project1.path, project1.name, addedToProject1),
                DatasetProject(project2.path, project2.name, addedToProject2),
                DatasetProject(project2Fork.path, project2Fork.name, addedToProject2)
              ).sorted
            )
          )
      }
    }

    "return details of the dataset with the given id " +
      "- a case when an in-project created dataset is defined on a project which has a fork" in new TestCase {
      forAll(datasetProjects, datasetProjects, addedToProjectObjects, commitIds) {
        (sourceProject, forkProject, addedToProject, commitId) =>
          val dataset = nonModifiedDatasets(
            projects = sourceProject.copy(created = addedToProject).toGenerator
          ).generateOne

          loadToStore(
            dataset.toJsonLD(noSameAs = true, commitId = commitId),
            dataset // to simulate forking the sourceProject
              .copy(projects = List(DatasetProject(forkProject.path, forkProject.name, addedToProject)))
              .toJsonLD(noSameAs = true, commitId = commitId)
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              sameAs = dataset.entityId.asSameAs,
              parts  = dataset.parts.sorted,
              projects = List(
                DatasetProject(sourceProject.path, sourceProject.name, addedToProject),
                DatasetProject(forkProject.path, forkProject.name, addedToProject)
              ).sorted
            )
          )
      }
    }

    "return details of the dataset with the given id " +
      "- a case when an imported dataset is defined on a project which has a fork" in new TestCase {
      forAll(datasetProjects, datasetProjects, addedToProjectObjects, commitIds) {
        (sourceProject, forkProject, addedToProject, commitId) =>
          val dataset = nonModifiedDatasets(
            projects = sourceProject.copy(created = addedToProject).toGenerator
          ).generateOne

          loadToStore(
            dataset.toJsonLD(commitId = commitId),
            dataset // to simulate forking the sourceProject
              .copy(projects = List(DatasetProject(forkProject.path, forkProject.name, addedToProject)))
              .toJsonLD(commitId = commitId)
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              parts = dataset.parts.sorted,
              projects = List(
                DatasetProject(sourceProject.path, sourceProject.name, addedToProject),
                DatasetProject(forkProject.path, forkProject.name, addedToProject)
              ).sorted
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
            datasetOnGrandparent.toJsonLD(noSameAs = true, commitId = commitId),
            datasetOnGrandparent // to simulate forking the grandparentProject - dataset on a parent project
              .copy(projects = List(DatasetProject(parentProject.path, parentProject.name, addedToProject)))
              .toJsonLD(noSameAs = true, commitId = commitId),
            datasetOnGrandparent // to simulate forking the parentProject - dataset on a child project
              .copy(projects = List(DatasetProject(childProject.path, childProject.name, addedToProject)))
              .toJsonLD(noSameAs = true, commitId = commitId)
          )

          datasetFinder.findDataset(datasetOnGrandparent.id).unsafeRunSync() shouldBe Some(
            datasetOnGrandparent.copy(
              sameAs = datasetOnGrandparent.entityId.asSameAs,
              parts  = datasetOnGrandparent.parts.sorted,
              projects = List(
                DatasetProject(grandparentProject.path, grandparentProject.name, addedToProject),
                DatasetProject(parentProject.path, parentProject.name, addedToProject),
                DatasetProject(childProject.path, childProject.name, addedToProject)
              ).sorted
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
          date = datasetInProjectCreationDates generateGreaterThan addedToProject.date
        )
        val modifiedDataset = modifiedDatasets(dataset, project.copy(created = modifiedOnProject)).generateOne.copy(
          maybeDescription = datasetDescriptions.generateSome
        )

        loadToStore(
          dataset.toJsonLD(),
          modifiedDataset.toJsonLD(commitId = projectDatasetModificationCommit), // to simulate modifying the data-set
          modifiedDataset // to simulate forking project after dataset modification
            .copy(projects = List(DatasetProject(forkedProject.path, forkedProject.name, modifiedOnProject)))
            .toJsonLD(commitId = projectDatasetModificationCommit)
        )

        datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
          dataset.copy(
            parts    = dataset.parts.sorted,
            projects = List(DatasetProject(project.path, project.name, addedToProject)).sorted
          )
        )
        datasetFinder.findDataset(modifiedDataset.id).unsafeRunSync() shouldBe Some(
          modifiedDataset.copy(
            parts = modifiedDataset.parts.sorted,
            projects = List(
              DatasetProject(project.path, project.name, modifiedOnProject),
              DatasetProject(forkedProject.path, forkedProject.name, modifiedOnProject)
            ).sorted
          )
        )
      }
    }
  }

  "findDataset in the case of dataset import hierarchy" should {

    "return details of the dataset with the given id " +
      "- case when the first dataset is imported from a third party provider" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = nonModifiedDatasets().generateOne.copy(
        projects = List(dataset1Project)
      )

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1.entityId.asSameAs,
        projects = List(dataset2Project)
      )

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1.toJsonLD(), dataset2.toJsonLD(), dataset3.toJsonLD())

      datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
        dataset1.copy(
          parts = dataset1.parts.sorted,
          projects = List(
            dataset1Project,
            dataset2Project,
            dataset3Project
          ).sorted
        )
      )
    }

    "return details of the dataset with the given id - case when the first dataset is in-project created" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = nonModifiedDatasets().generateOne
        .copy(projects = List(dataset1Project))

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1.entityId.asSameAs,
        projects = List(dataset2Project)
      )

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1.toJsonLD(noSameAs = true), dataset2.toJsonLD(), dataset3.toJsonLD())

      datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
        dataset1.copy(
          sameAs = dataset1.entityId.asSameAs,
          parts  = dataset1.parts.sorted,
          projects = List(
            dataset1Project,
            dataset2Project,
            dataset3Project
          ).sorted
        )
      )
    }

    "return details of the dataset with the given id - " +
      "case when the requested id is in the middle of the hierarchy" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = nonModifiedDatasets().generateOne
        .copy(projects = List(dataset1Project))

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1.entityId.asSameAs,
        projects = List(dataset2Project)
      )

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1.toJsonLD(noSameAs = true), dataset2.toJsonLD(), dataset3.toJsonLD())

      datasetFinder.findDataset(dataset2.id).unsafeRunSync() shouldBe Some(
        dataset2.copy(
          sameAs = dataset1.entityId.asSameAs,
          parts  = dataset2.parts.sorted,
          projects = List(
            dataset1Project,
            dataset2Project,
            dataset3Project
          ).sorted
        )
      )
    }

    "return details of the dataset with the given id - " +
      "case when the requested id is at the end of the hierarchy" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = nonModifiedDatasets().generateOne
        .copy(projects = List(dataset1Project))

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1.entityId.asSameAs,
        projects = List(dataset2Project)
      )

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1.toJsonLD(noSameAs = true), dataset2.toJsonLD(), dataset3.toJsonLD())

      datasetFinder.findDataset(dataset3.id).unsafeRunSync() shouldBe Some(
        dataset3.copy(
          sameAs = dataset1.entityId.asSameAs,
          parts  = dataset3.parts.sorted,
          projects = List(
            dataset1Project,
            dataset2Project,
            dataset3Project
          ).sorted
        )
      )
    }

    "return details of the dataset with the given id - " +
      "case when there're two first level projects sharing a dataset " +
      "and some other project imports from on of these two" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = nonModifiedDatasets().generateOne
        .copy(projects = List(dataset1Project))

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = nonModifiedDatasets().generateOne.copy(
        sameAs   = dataset1.sameAs,
        projects = List(dataset2Project)
      )

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1.toJsonLD(), dataset2.toJsonLD(), dataset3.toJsonLD())

      datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
        dataset1.copy(
          parts = dataset1.parts.sorted,
          projects = List(
            dataset1Project,
            dataset2Project,
            dataset3Project
          ).sorted
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
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1.entityId.asSameAs,
        projects = List(dataset2Project)
      )

      val modifiedOnProject2 = addedToProjectObjects.generateOne.copy(
        date = datasetInProjectCreationDates generateGreaterThan dataset2Project.created.date
      )
      private val dataset2ModifiedProject = dataset2Project.copy(created = modifiedOnProject2)
      val modifiedDataset2 = modifiedDatasets(dataset2, dataset2ModifiedProject).generateOne.copy(
        maybeDescription = datasetDescriptions.generateSome
      )

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project.copy(
        created = modifiedOnProject2
      )
      val dataset3 = NonModifiedDataset(
        id               = datasetIdentifiers.generateOne,
        name             = modifiedDataset2.name,
        url              = datasetUrls.generateOne,
        sameAs           = modifiedDataset2.entityId.asSameAs,
        maybeDescription = datasetDescriptions.generateSome,
        published        = modifiedDataset2.published,
        parts            = modifiedDataset2.parts,
        projects         = List(dataset3Project)
      )

      loadToStore(dataset1.toJsonLD(), dataset2.toJsonLD(), modifiedDataset2.toJsonLD(), dataset3.toJsonLD())

      datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
        dataset1.copy(
          parts    = dataset1.parts.sorted,
          projects = List(dataset1Project, dataset2Project).sorted
        )
      )
      datasetFinder.findDataset(modifiedDataset2.id).unsafeRunSync() shouldBe Some(
        modifiedDataset2.copy(
          parts    = modifiedDataset2.parts.sorted,
          projects = List(dataset2ModifiedProject, dataset3Project).sorted
        )
      )
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetFinder = new IODatasetFinder(
      new BaseDetailsFinder(rdfStoreConfig, logger, timeRecorder),
      new CreatorsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder),
      new PartsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder),
      new ProjectsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
    )
  }

  private implicit class DatasetProjectOps(datasetProject: DatasetProject) {
    def shiftDateAfter(project: DatasetProject): DatasetProject =
      datasetProject.copy(
        created = datasetProject.created.copy(
          date = datasetInProjectCreationDates generateGreaterThan project.created.date
        )
      )

    lazy val toGenerator: Gen[NonEmptyList[DatasetProject]] = Gen.const(NonEmptyList of datasetProject)
  }

  private implicit class EntityIdOps(entityId: EntityId) {
    lazy val asSameAs: SameAs = SameAs.fromId(entityId.value.toString).fold(throw _, identity)
  }

  private implicit class OptionEntityIdOps(maybeEntityId: Option[EntityId]) {
    lazy val asSameAs: SameAs = maybeEntityId
      .flatMap(id => SameAs.fromId(id.value.toString).toOption)
      .getOrElse(throw new Exception(s"Cannot convert $maybeEntityId EntityId to SameAs"))
  }

  private implicit class NonModifiedDatasetOps(dataSet: NonModifiedDataset) {

    lazy val entityId: EntityId = DataSet.entityId(dataSet.id)

    def toJsonLD(
        noSameAs: Boolean  = false,
        commitId: CommitId = commitIds.generateOne
    ): JsonLD = dataSet.projects match {
      case project +: Nil =>
        nonModifiedDataSetCommit(
          commitId      = commitId,
          committedDate = CommittedDate(project.created.date.value),
          committer     = Person(project.created.agent.name, project.created.agent.maybeEmail)
        )(
          projectPath = project.path,
          projectName = project.name
        )(
          datasetIdentifier         = dataSet.id,
          datasetName               = dataSet.name,
          datasetUrl                = dataSet.url,
          maybeDatasetSameAs        = if (noSameAs) None else dataSet.sameAs.some,
          maybeDatasetDescription   = dataSet.maybeDescription,
          maybeDatasetPublishedDate = dataSet.published.maybeDate,
          datasetCreatedDate        = DateCreated(project.created.date.value),
          datasetCreators           = dataSet.published.creators map toPerson,
          datasetParts              = dataSet.parts.map(part => (part.name, part.atLocation))
        )
      case _ => fail("Not prepared to work datasets having multiple projects")
    }
  }

  private implicit class ModifiedDatasetOps(dataSet: ModifiedDataset) {

    lazy val entityId: EntityId = DataSet.entityId(dataSet.id)

    def toJsonLD(commitId: CommitId = commitIds.generateOne): JsonLD = dataSet.projects match {
      case project +: Nil =>
        modifiedDataSetCommit(
          commitId      = commitId,
          committedDate = CommittedDate(project.created.date.value),
          committer     = Person(project.created.agent.name, project.created.agent.maybeEmail)
        )(
          projectPath = project.path,
          projectName = project.name
        )(
          datasetIdentifier         = dataSet.id,
          datasetName               = dataSet.name,
          datasetUrl                = dataSet.url,
          datasetDerivedFrom        = dataSet.derivedFrom,
          maybeDatasetDescription   = dataSet.maybeDescription,
          maybeDatasetPublishedDate = dataSet.published.maybeDate,
          datasetCreatedDate        = DateCreated(project.created.date.value),
          datasetCreators           = dataSet.published.creators map toPerson,
          datasetParts              = dataSet.parts.map(part => (part.name, part.atLocation))
        )
      case _ => fail("Not prepared to work datasets having multiple projects")
    }
  }

  private lazy val toPerson: DatasetCreator => Person =
    creator => Person(creator.name, creator.maybeEmail, creator.maybeAffiliation)

  private implicit lazy val partsAlphabeticalOrdering: Ordering[DatasetPart] =
    (part1: DatasetPart, part2: DatasetPart) => part1.name.value compareTo part2.name.value

  private implicit lazy val projectsAlphabeticalOrdering: Ordering[DatasetProject] =
    (project1: DatasetProject, project2: DatasetProject) => project1.name.value compareTo project2.name.value

  private implicit val dateCreatedInProjectOrdering: Ordering[DateCreatedInProject] =
    (x: DateCreatedInProject, y: DateCreatedInProject) => x.value compareTo y.value
}
