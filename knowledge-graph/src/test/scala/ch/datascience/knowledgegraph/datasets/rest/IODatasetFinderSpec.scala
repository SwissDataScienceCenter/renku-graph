/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import ch.datascience.generators.Generators.httpUrls
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{DatasetPart, DatasetProject}
import ch.datascience.knowledgegraph.datasets.{CreatorsFinder, PartsFinder, ProjectsFinder}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.stubbing.ExternalServiceStubbing
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetFinderSpec
    extends WordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  "findDataset" should {

    "return the details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset" in new InMemoryStoreTestCase {
      forAll(datasets, projectPaths, datasetInProjectCreations, projectPaths, datasetInProjectCreations) {
        (dataset, project1Path, project1DatasetCreation, project2Path, project2DatasetCreation) =>
          val reusedDatasetUrl = (for {
            url  <- httpUrls
            uuid <- Gen.uuid
          } yield s"$url/$uuid").generateOne

          loadToStore(
            triples(
              singleFileAndCommitWithDataset(
                project1Path,
                commitIds.generateOne,
                committerName             = project1DatasetCreation.agent.name,
                committerEmail            = project1DatasetCreation.agent.email,
                committedDate             = project1DatasetCreation.date.toUnsafe(date => CommittedDate.from(date.value)),
                datasetIdentifier         = dataset.id,
                datasetName               = dataset.name,
                maybeDatasetDescription   = dataset.maybeDescription,
                maybeDatasetPublishedDate = dataset.published.maybeDate,
                maybeDatasetCreators      = dataset.published.creators.map(creator => (creator.name, creator.maybeEmail)),
                maybeDatasetParts         = dataset.part.map(part => (part.name, part.atLocation)),
                maybeDatasetUrl           = Some(reusedDatasetUrl)
              ),
              singleFileAndCommitWithDataset(
                project2Path,
                commitIds.generateOne,
                committerName             = project2DatasetCreation.agent.name,
                committerEmail            = project2DatasetCreation.agent.email,
                committedDate             = project2DatasetCreation.date.toUnsafe(date => CommittedDate.from(date.value)),
                datasetIdentifier         = dataset.id,
                datasetName               = dataset.name,
                maybeDatasetDescription   = dataset.maybeDescription,
                maybeDatasetPublishedDate = dataset.published.maybeDate,
                maybeDatasetCreators      = dataset.published.creators.map(creator => (creator.name, creator.maybeEmail)),
                maybeDatasetParts         = dataset.part.map(part => (part.name, part.atLocation)),
                maybeDatasetUrl           = Some(reusedDatasetUrl)
              ),
              singleFileAndCommitWithDataset(projectPaths.generateOne)
            )
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              part = dataset.part.sorted,
              project = List(DatasetProject(project1Path, project1DatasetCreation),
                             DatasetProject(project2Path, project2DatasetCreation)).sorted
            )
          )
      }
    }

    "return the details of the dataset with the given id " +
      "- a case when a dataset is defined on a source project which has a fork" in new InMemoryStoreTestCase {
      forAll(projectPaths, projectPaths, datasets, datasetInProjectCreations, commitIds) {
        (sourceProjectPath, forkProjectPath, dataset, projectDatasetCreation, commitId) =>
          val reusedDatasetUrl = (for {
            url  <- httpUrls
            uuid <- Gen.uuid
          } yield s"$url/$uuid").generateOne

          loadToStore(
            triples(
              singleFileAndCommitWithDataset(
                sourceProjectPath,
                commitId,
                committerName             = projectDatasetCreation.agent.name,
                committerEmail            = projectDatasetCreation.agent.email,
                committedDate             = projectDatasetCreation.date.toUnsafe(date => CommittedDate.from(date.value)),
                datasetIdentifier         = dataset.id,
                datasetName               = dataset.name,
                maybeDatasetDescription   = dataset.maybeDescription,
                maybeDatasetPublishedDate = dataset.published.maybeDate,
                maybeDatasetCreators      = dataset.published.creators.map(creator => (creator.name, creator.maybeEmail)),
                maybeDatasetParts         = dataset.part.map(part => (part.name, part.atLocation)),
                maybeDatasetUrl           = Some(reusedDatasetUrl)
              ),
              singleFileAndCommitWithDataset(
                forkProjectPath,
                commitId,
                committerName             = projectDatasetCreation.agent.name,
                committerEmail            = projectDatasetCreation.agent.email,
                committedDate             = projectDatasetCreation.date.toUnsafe(date => CommittedDate.from(date.value)),
                datasetIdentifier         = dataset.id,
                datasetName               = dataset.name,
                maybeDatasetDescription   = dataset.maybeDescription,
                maybeDatasetPublishedDate = dataset.published.maybeDate,
                maybeDatasetCreators      = dataset.published.creators.map(creator => (creator.name, creator.maybeEmail)),
                maybeDatasetParts         = dataset.part.map(part => (part.name, part.atLocation)),
                maybeDatasetUrl           = Some(reusedDatasetUrl)
              ),
              singleFileAndCommitWithDataset(projectPaths.generateOne)
            )
          )

          datasetFinder.findDataset(dataset.id).unsafeRunSync() shouldBe Some(
            dataset.copy(
              part = dataset.part.sorted,
              project = List(DatasetProject(sourceProjectPath, projectDatasetCreation),
                             DatasetProject(forkProjectPath, projectDatasetCreation)).sorted
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

  private implicit lazy val partsAlphabeticalOrdering: Ordering[DatasetPart] =
    (part1: DatasetPart, part2: DatasetPart) => part1.name.value compareTo part2.name.value

  private implicit lazy val projectsAlphabeticalOrdering: Ordering[DatasetProject] =
    (project1: DatasetProject, project2: DatasetProject) => project1.name.value compareTo project2.name.value
}
