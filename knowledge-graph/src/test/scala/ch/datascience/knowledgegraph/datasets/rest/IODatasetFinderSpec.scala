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

import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DateCreated, DateCreatedInProject, SameAs}
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.entities.{DataSet, Person}
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.{EntityId, JsonLD}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetFinderSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "findDataset" should {

    "return the details of the dataset with the given id " +
      "- a case when unrelated projects are using the same imported dataset" in new TestCase {
      forAll(datasets, datasetProjects, addedToProject, datasetProjects, addedToProject) {
        (dataset, project1, addedToProject1, project2, addedToProject2) =>
          val project1DatasetCreationDate = CommittedDate(addedToProject1.date.value)

          loadToStore(
            dataSetCommit(
              committedDate = project1DatasetCreationDate,
              committer     = Person(addedToProject1.agent.name, addedToProject1.agent.email)
            )(
              project1.path,
              project1.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.sameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreatedDate        = DateCreated(addedToProject1.date.value),
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to simulate adding a file to the data-set in another commit
              committedDate = CommittedDate(project1DatasetCreationDate.value plusSeconds 10)
            )(
              project1.path,
              project1.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.sameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreatedDate        = DateCreated(addedToProject1.date.value),
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to simulate adding the same data-set to another project
              committedDate = addedToProject2.date.toUnsafe(date => CommittedDate.from(date.value)),
              committer     = Person(addedToProject2.agent.name, addedToProject2.agent.email)
            )(
              project2.path,
              project2.name
            )(
              datasetIdentifier         = datasetIdentifiers.generateOne,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.sameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            randomDataSetCommit
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              parts = dataset.parts.sorted,
              projects = List(DatasetProject(project1.path, project1.name, addedToProject1),
                              DatasetProject(project2.path, project2.name, addedToProject2)).sorted
            )
          )
      }
    }

    "return the details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset created in renku project" in new TestCase {
      forAll(datasets, datasetProjects, addedToProject, datasetProjects, addedToProject) {
        (dataset, project1, addedToProject1, project2, addedToProject2) =>
          loadToStore(
            dataSetCommit(
              committedDate = CommittedDate(addedToProject1.date.value),
              committer     = Person(addedToProject1.agent.name, addedToProject1.agent.email)
            )(
              project1.path,
              project1.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = None,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreatedDate        = DateCreated(addedToProject1.date.value),
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // simulating dataset modification
              committedDate = CommittedDate(addedToProject1.date.value).shiftToFuture
            )(
              project1.path,
              project1.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = None,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreatedDate        = DateCreated(addedToProject1.date.value),
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to simulate adding first project's data-set to another project
              committedDate = addedToProject2.date.toUnsafe(date => CommittedDate.from(date.value)),
              committer     = Person(addedToProject2.agent.name, addedToProject2.agent.email)
            )(
              project2.path,
              project2.name
            )(
              datasetIdentifier         = datasetIdentifiers.generateOne,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.entityId.asSameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
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
      }
    }

    "return the details of the dataset with the given id " +
      "- a case when an imported dataset is used in one project" in new TestCase {
      forAll(datasets, datasetProjects, addedToProject) { (dataset, project, addedToProject) =>
        loadToStore(
          dataSetCommit(
            committedDate = CommittedDate(addedToProject.date.value),
            committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
          )(
            project.path,
            project.name
          )(
            datasetIdentifier         = dataset.id,
            datasetName               = dataset.name,
            maybeDatasetUrl           = dataset.maybeUrl,
            maybeDatasetSameAs        = dataset.sameAs.some,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreatedDate        = DateCreated(addedToProject.date.value),
            datasetCreators           = dataset.published.creators map toPerson,
            datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
          ),
          dataSetCommit( // simulating dataset modification
            committedDate = CommittedDate(addedToProject.date.value).shiftToFuture
          )(
            project.path,
            project.name
          )(
            datasetIdentifier         = dataset.id,
            datasetName               = dataset.name,
            maybeDatasetUrl           = dataset.maybeUrl,
            maybeDatasetSameAs        = dataset.sameAs.some,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreatedDate        = DateCreated(addedToProject.date.value),
            datasetCreators           = dataset.published.creators map toPerson,
            datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
          ),
          randomDataSetCommit
        )

        datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
          dataset.copy(
            parts    = dataset.parts.sorted,
            projects = List(DatasetProject(project.path, project.name, addedToProject))
          )
        )
      }
    }

    "return the details of the dataset with the given id " +
      "- a case of a single used in-project created dataset" in new TestCase {
      forAll(datasets, datasetProjects, addedToProject) { (dataset, project, addedToProject) =>
        loadToStore(
          dataSetCommit(
            committedDate = CommittedDate(addedToProject.date.value),
            committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
          )(
            project.path,
            project.name
          )(
            datasetIdentifier         = dataset.id,
            datasetName               = dataset.name,
            maybeDatasetUrl           = dataset.maybeUrl,
            maybeDatasetSameAs        = None,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreatedDate        = DateCreated(addedToProject.date.value),
            datasetCreators           = dataset.published.creators map toPerson,
            datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
          ),
          dataSetCommit( // simulating dataset modification
            committedDate = CommittedDate(addedToProject.date.value).shiftToFuture
          )(
            project.path,
            project.name
          )(
            datasetIdentifier         = dataset.id,
            datasetName               = dataset.name,
            maybeDatasetUrl           = dataset.maybeUrl,
            maybeDatasetSameAs        = None,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreatedDate        = DateCreated(addedToProject.date.value),
            datasetCreators           = dataset.published.creators map toPerson,
            datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
          ),
          randomDataSetCommit
        )

        datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
          dataset.copy(
            sameAs   = dataset.entityId.asSameAs,
            parts    = dataset.parts.sorted,
            projects = List(DatasetProject(project.path, project.name, addedToProject))
          )
        )
      }
    }
  }

  "findDataset in case of forks" should {

    "return the details of the dataset with the given id " +
      "- a case when unrelated projects are using the same imported dataset and one of them is forked" in new TestCase {
      forAll(datasets, datasetProjects, addedToProject, datasetProjects, addedToProject, datasetProjects) {
        (dataset, project1, addedToProject1, project2, addedToProject2, project2Fork) =>
          val project1DatasetCreationDate = CommittedDate(addedToProject1.date.value)
          val project2DatasetCommit       = commitIds.generateOne
          val project2DatasetCommitDate   = addedToProject2.date.toUnsafe(date => CommittedDate.from(date.value))
          val project2DatasetId           = datasetIdentifiers.generateOne

          loadToStore(
            dataSetCommit(
              committedDate = project1DatasetCreationDate,
              committer     = Person(addedToProject1.agent.name, addedToProject1.agent.email)
            )(
              project1.path,
              project1.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.sameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to simulate adding the same data-set to another project
              commitId      = project2DatasetCommit,
              committedDate = project2DatasetCommitDate,
              committer     = Person(addedToProject2.agent.name, addedToProject2.agent.email)
            )(
              project2.path,
              project2.name
            )(
              datasetIdentifier         = project2DatasetId,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.sameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to simulate forking project2
              commitId      = project2DatasetCommit,
              committedDate = project2DatasetCommitDate,
              committer     = Person(addedToProject2.agent.name, addedToProject2.agent.email)
            )(
              project2Fork.path,
              project2Fork.name
            )(
              datasetIdentifier         = project2DatasetId,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.sameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            )
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

    "return None if there's no datasets with the given id" in new TestCase {
      val identifier = datasetIdentifiers.generateOne
      datasetFinder.findDataset(identifier).unsafeRunSync() shouldBe None
    }

    "return the details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset created in renku project and one of them is forked" in new TestCase {
      forAll(datasets, datasetProjects, addedToProject, datasetProjects, addedToProject, datasetProjects) {
        (dataset, project1, addedToProject1, project2, addedToProject2, project2Fork) =>
          val project1DatasetCreationDate = CommittedDate(addedToProject1.date.value)
          val project2DatasetCommit       = commitIds.generateOne
          val project2DatasetCommitDate   = addedToProject2.date.toUnsafe(date => CommittedDate.from(date.value))
          val project2DatasetId           = datasetIdentifiers.generateOne

          loadToStore(
            dataSetCommit(
              committedDate = project1DatasetCreationDate,
              committer     = Person(addedToProject1.agent.name, addedToProject1.agent.email)
            )(
              project1.path,
              project1.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = None,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to simulate adding first project's data-set to another project
              commitId      = project2DatasetCommit,
              committedDate = project2DatasetCommitDate,
              committer     = Person(addedToProject2.agent.name, addedToProject2.agent.email)
            )(
              project2.path,
              project2.name
            )(
              datasetIdentifier         = project2DatasetId,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.entityId.asSameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to simulate forking project2
              commitId      = project2DatasetCommit,
              committedDate = project2DatasetCommitDate,
              committer     = Person(addedToProject2.agent.name, addedToProject2.agent.email)
            )(
              project2Fork.path,
              project2Fork.name
            )(
              datasetIdentifier         = project2DatasetId,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.entityId.asSameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            )
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

    "return the details of the dataset with the given id " +
      "- a case when a created dataset is defined on a project which has a fork" in new TestCase {
      forAll(datasetProjects, datasetProjects, datasets, addedToProject, commitIds) {
        (sourceProject, forkProject, dataset, addedToProject, commitId) =>
          val datasetCreationDate = addedToProject.date.toUnsafe(date => CommittedDate.from(date.value))
          loadToStore(
            dataSetCommit(
              commitId      = commitId,
              committedDate = datasetCreationDate,
              committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
            )(
              sourceProject.path,
              sourceProject.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = None,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit(
              commitId      = commitId,
              committedDate = datasetCreationDate,
              committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
            )(
              forkProject.path,
              forkProject.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = None,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            )
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

    "return the details of the dataset with the given id " +
      "- a case when an imported dataset is defined on a project which has a fork" in new TestCase {
      forAll(datasetProjects, datasetProjects, datasets, addedToProject, commitIds) {
        (sourceProject, forkProject, dataset, addedToProject, commitId) =>
          val datasetCreationDate = addedToProject.date.toUnsafe(date => CommittedDate.from(date.value))
          loadToStore(
            dataSetCommit(
              commitId      = commitId,
              committedDate = datasetCreationDate,
              committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
            )(
              sourceProject.path,
              sourceProject.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.sameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit(
              commitId      = commitId,
              committedDate = datasetCreationDate,
              committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
            )(
              forkProject.path,
              forkProject.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.sameAs.some,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            )
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

    "return the details of the dataset with the given id " +
      "- a case when a created dataset is defined on a grandparent project which has two levels of forks" in new TestCase {
      forAll(datasetProjects, datasetProjects, datasetProjects, datasets, addedToProject, commitIds) {
        (grandparentProject, parentProject, childProject, dataset, addedToProject, commitId) =>
          val datasetCreationDate = addedToProject.date.toUnsafe(date => CommittedDate.from(date.value))
          val grandparentProjectDataSet = dataSetCommit(
            commitId      = commitId,
            committedDate = datasetCreationDate,
            committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
          )(
            grandparentProject.path,
            grandparentProject.name
          )(
            datasetIdentifier         = dataset.id,
            datasetName               = dataset.name,
            maybeDatasetUrl           = dataset.maybeUrl,
            maybeDatasetSameAs        = None,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreators           = dataset.published.creators map toPerson,
            datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
          )
          val parentProjectDataSet = dataSetCommit(
            commitId      = commitId,
            committedDate = datasetCreationDate,
            committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
          )(
            parentProject.path,
            parentProject.name
          )(
            datasetIdentifier         = dataset.id,
            datasetName               = dataset.name,
            maybeDatasetUrl           = dataset.maybeUrl,
            maybeDatasetSameAs        = None,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreators           = dataset.published.creators map toPerson,
            datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
          )
          loadToStore(
            grandparentProjectDataSet,
            parentProjectDataSet,
            dataSetCommit(
              commitId      = commitId,
              committedDate = datasetCreationDate,
              committer     = Person(addedToProject.agent.name, addedToProject.agent.email)
            )(
              childProject.path,
              childProject.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = None,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.parts.map(part => (part.name, part.atLocation))
            )
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              sameAs = dataset.entityId.asSameAs,
              parts  = dataset.parts.sorted,
              projects = List(
                DatasetProject(grandparentProject.path, grandparentProject.name, addedToProject),
                DatasetProject(parentProject.path, parentProject.name, addedToProject),
                DatasetProject(childProject.path, childProject.name, addedToProject)
              ).sorted
            )
          )
      }
    }
  }

  "findDataset in case of import hierarchy" should {

    "return the details of the dataset with the given id - case when the first dataset is externally imported" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = datasets.generateOne
        .copy(projects = List(dataset1Project))
      val dataset1Json = toDataSetCommit(dataset1)

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1Json.entityId.asSameAs,
        projects = List(dataset2Project)
      )
      val dataset2Json = toDataSetCommit(dataset2)

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2Json.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1Json, dataset2Json, toDataSetCommit(dataset3))

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

    "return the details of the dataset with the given id - case when the first dataset is in-project created" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = datasets.generateOne
        .copy(projects = List(dataset1Project))
      val dataset1Json = toDataSetCommit(dataset1, noSameAs = true)

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1Json.entityId.asSameAs,
        projects = List(dataset2Project)
      )
      val dataset2Json = toDataSetCommit(dataset2)

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2Json.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1Json, dataset2Json, toDataSetCommit(dataset3))

      datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
        dataset1.copy(
          sameAs = dataset1Json.entityId.asSameAs,
          parts  = dataset1.parts.sorted,
          projects = List(
            dataset1Project,
            dataset2Project,
            dataset3Project
          ).sorted
        )
      )
    }

    "return the details of the dataset with the given id - " +
      "case when the requested id is in the middle of the hierarchy" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = datasets.generateOne
        .copy(projects = List(dataset1Project))
      val dataset1Json = toDataSetCommit(dataset1, noSameAs = true)

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1Json.entityId.asSameAs,
        projects = List(dataset2Project)
      )
      val dataset2Json = toDataSetCommit(dataset2)

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2Json.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1Json, dataset2Json, toDataSetCommit(dataset3))

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

    "return the details of the dataset with the given id - " +
      "case when the requested id is at the end of the hierarchy" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = datasets.generateOne
        .copy(projects = List(dataset1Project))
      val dataset1Json = toDataSetCommit(dataset1, noSameAs = true)

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset1Json.entityId.asSameAs,
        projects = List(dataset2Project)
      )
      val dataset2Json = toDataSetCommit(dataset2)

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2Json.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1Json, dataset2Json, toDataSetCommit(dataset3))

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

    "return the details of the dataset with the given id - " +
      "case when there're two first level projects sharing a dataset " +
      "and some other project imports from on of these two" in new TestCase {
      val dataset1Project = datasetProjects.generateOne
      val dataset1 = datasets.generateOne
        .copy(projects = List(dataset1Project))
      val dataset1Json = toDataSetCommit(dataset1)

      val dataset2Project = datasetProjects.generateOne shiftDateAfter dataset1Project
      val dataset2 = datasets.generateOne.copy(
        sameAs   = dataset1.sameAs,
        projects = List(dataset2Project)
      )
      val dataset2Json = toDataSetCommit(dataset2)

      val dataset3Project = datasetProjects.generateOne shiftDateAfter dataset2Project
      val dataset3 = dataset2.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset2Json.entityId.asSameAs,
        projects = List(dataset3Project)
      )

      loadToStore(dataset1Json, dataset2Json, toDataSetCommit(dataset3))

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
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetFinder = new IODatasetFinder(
      new BaseDetailsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder),
      new CreatorsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder),
      new PartsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder),
      new ProjectsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
    )
  }

  private implicit class CommittedDateOps(date: CommittedDate) {
    lazy val shiftToFuture = CommittedDate(date.value plusSeconds positiveInts().generateOne.value)
  }

  private implicit class DatasetProjectOps(datasetProject: DatasetProject) {
    def shiftDateAfter(project: DatasetProject): DatasetProject =
      datasetProject.copy(
        created = datasetProject.created.copy(
          date = DateCreatedInProject(project.created.date.value plusSeconds positiveInts().generateOne.value)
        )
      )
  }

  private implicit class DatasetOps(dataset: Dataset) {
    lazy val entityId: EntityId = DataSet.entityId(dataset.id)
  }

  private implicit class EntityIdOps(entityId: EntityId) {
    lazy val asSameAs: SameAs = SameAs.fromId(entityId.value).fold(throw _, identity)
  }

  private implicit class OptionEntityIdOps(maybeEntityId: Option[EntityId]) {
    lazy val asSameAs: SameAs = maybeEntityId
      .flatMap(id => SameAs.fromId(id.value).toOption)
      .getOrElse(throw new Exception(s"Cannot convert $maybeEntityId EntityId to SameAs"))
  }

  private def toDataSetCommit(dataSet: Dataset, noSameAs: Boolean = false): JsonLD =
    dataSet.projects match {
      case project +: Nil =>
        dataSetCommit(
          committedDate = CommittedDate(project.created.date.value),
          committer     = Person(project.created.agent.name, project.created.agent.email)
        )(
          projectPath = project.path,
          projectName = project.name
        )(
          datasetIdentifier         = dataSet.id,
          datasetName               = dataSet.name,
          maybeDatasetUrl           = dataSet.maybeUrl,
          maybeDatasetSameAs        = if (noSameAs) None else dataSet.sameAs.some,
          maybeDatasetDescription   = dataSet.maybeDescription,
          maybeDatasetPublishedDate = dataSet.published.maybeDate,
          datasetCreatedDate        = DateCreated(project.created.date.value),
          datasetCreators           = dataSet.published.creators map toPerson,
          datasetParts              = dataSet.parts.map(part => (part.name, part.atLocation))
        )
      case _ => fail("Not prepared to work datasets having multiple projects")
    }

  private lazy val toPerson: DatasetCreator => Person =
    creator => Person(creator.name, creator.maybeEmail, creator.maybeAffiliation)

  private implicit lazy val partsAlphabeticalOrdering: Ordering[DatasetPart] =
    (part1: DatasetPart, part2: DatasetPart) => part1.name.value compareTo part2.name.value

  private implicit lazy val projectsAlphabeticalOrdering: Ordering[DatasetProject] =
    (project1: DatasetProject, project2: DatasetProject) => project1.name.value compareTo project2.name.value
}
