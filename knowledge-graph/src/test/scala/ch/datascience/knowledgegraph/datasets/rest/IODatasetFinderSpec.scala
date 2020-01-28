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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{DatasetCreator, DatasetPart, DatasetProject}
import ch.datascience.knowledgegraph.datasets.{CreatorsFinder, PartsFinder, ProjectsFinder}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetFinderSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "findDataset" should {

    "return the details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset" in new InMemoryStoreTestCase {
      forAll(datasets, datasetProjects, datasetInProjectCreations, datasetProjects, datasetInProjectCreations) {
        (dataset, project1, project1DatasetCreation, project2, project2DatasetCreation) =>
          val project1DatasetCreationDate = CommittedDate(project1DatasetCreation.date.value)

          loadToStore(
            dataSetCommit(
              committedDate = project1DatasetCreationDate,
              committer     = Person(project1DatasetCreation.agent.name, project1DatasetCreation.agent.email)
            )(
              project1.path,
              project1.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.maybeSameAs,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.part.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to reflect a file added to the data-set in another commit
              committedDate = CommittedDate(project1DatasetCreationDate.value plusSeconds 10)
            )(
              project1.path,
              project1.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.maybeSameAs,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.part.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit(
              committedDate = project2DatasetCreation.date.toUnsafe(date => CommittedDate.from(date.value)),
              committer     = Person(project2DatasetCreation.agent.name, project2DatasetCreation.agent.email)
            )(
              project2.path,
              project2.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.maybeSameAs,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.part.map(part => (part.name, part.atLocation))
            ),
            randomDataSetCommit
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              part = dataset.part.sorted,
              project = List(DatasetProject(project1.path, project1.name, project1DatasetCreation),
                             DatasetProject(project2.path, project2.name, project2DatasetCreation)).sorted
            )
          )
      }
    }

    "return the details of the dataset with the given id " +
      "- a case when a dataset is defined on a source project which has a fork" in new InMemoryStoreTestCase {
      forAll(datasetProjects, datasetProjects, datasets, datasetInProjectCreations, commitIds) {
        (sourceProject, forkProject, dataset, projectDatasetCreation, commitId) =>
          val datasetCreationDate = projectDatasetCreation.date.toUnsafe(date => CommittedDate.from(date.value))
          loadToStore(
            dataSetCommit(
              commitId      = commitId,
              committedDate = datasetCreationDate,
              committer     = Person(projectDatasetCreation.agent.name, projectDatasetCreation.agent.email)
            )(
              sourceProject.path,
              sourceProject.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.maybeSameAs,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.part.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit( // to reflect a file added later to the dataset in another commit
              committedDate = CommittedDate(datasetCreationDate.value plusSeconds 10)
            )(
              sourceProject.path,
              sourceProject.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.maybeSameAs,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.part.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit(
              commitId      = commitId,
              committedDate = datasetCreationDate,
              committer     = Person(projectDatasetCreation.agent.name, projectDatasetCreation.agent.email)
            )(
              forkProject.path,
              forkProject.name
            )(
              datasetIdentifier         = dataset.id,
              datasetName               = dataset.name,
              maybeDatasetUrl           = dataset.maybeUrl,
              maybeDatasetSameAs        = dataset.maybeSameAs,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreators           = dataset.published.creators map toPerson,
              datasetParts              = dataset.part.map(part => (part.name, part.atLocation))
            )
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              part = dataset.part.sorted,
              project = List(
                DatasetProject(sourceProject.path, sourceProject.name, projectDatasetCreation),
                DatasetProject(forkProject.path, forkProject.name, projectDatasetCreation)
              ).sorted
            )
          )
      }
    }

    "return None if there's no datasets with the given id" in new InMemoryStoreTestCase {
      val identifier = datasetIds.generateOne
      datasetFinder.findDataset(identifier).unsafeRunSync() shouldBe None
    }
  }

  private trait InMemoryStoreTestCase {
    private val logger = TestLogger[IO]()
    val datasetFinder = new IODatasetFinder(
      new BaseDetailsFinder(rdfStoreConfig, renkuBaseUrl, logger),
      new CreatorsFinder(rdfStoreConfig, renkuBaseUrl, logger),
      new PartsFinder(rdfStoreConfig, renkuBaseUrl, logger),
      new ProjectsFinder(rdfStoreConfig, renkuBaseUrl, logger)
    )
  }

  private lazy val toPerson: DatasetCreator => Person =
    creator => Person(creator.name, creator.maybeEmail, creator.maybeAffiliation)

  private implicit lazy val partsAlphabeticalOrdering: Ordering[DatasetPart] =
    (part1: DatasetPart, part2: DatasetPart) => part1.name.value compareTo part2.name.value

  private implicit lazy val projectsAlphabeticalOrdering: Ordering[DatasetProject] =
    (project1: DatasetProject, project2: DatasetProject) => project1.name.value compareTo project2.name.value
}
