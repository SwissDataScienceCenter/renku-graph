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

package ch.datascience.knowledgegraph.datasets.graphql

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.httpUrls
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators.datasets
import ch.datascience.knowledgegraph.datasets.model.{DatasetPart, DatasetProject}
import ch.datascience.knowledgegraph.datasets.{CreatorsFinder, PartsFinder, ProjectsFinder}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.RdfStoreData._
import ch.datascience.stubbing.ExternalServiceStubbing
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IOProjectDatasetsFinderSpec
    extends WordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  "findDatasets" should {

    "return all datasets of the given project" in new InMemoryStoreTestCase {
      forAll(projectPaths, datasets, datasets) { (projectPath, dataset1, dataset2) =>
        val otherProject = projectPaths.generateOne
        val reusedDatasetUrl = (for {
          url  <- httpUrls
          uuid <- Gen.uuid
        } yield s"$url/$uuid").generateOne

        loadToStore(
          RDF(
            singleFileAndCommitWithDataset(otherProject,
                                           datasetId       = datasetIds.generateOne,
                                           datasetName     = datasetNames.generateOne,
                                           maybeDatasetUrl = Some(reusedDatasetUrl)),
            singleFileAndCommitWithDataset(
              projectPath,
              committerEmail            = dataset1.created.agent.email,
              committerName             = dataset1.created.agent.name,
              datasetId                 = dataset1.id,
              datasetName               = dataset1.name,
              maybeDatasetDescription   = dataset1.maybeDescription,
              datasetCreatedDate        = dataset1.created.date,
              maybeDatasetPublishedDate = dataset1.published.maybeDate,
              maybeDatasetCreators      = dataset1.published.creators.map(creator => (creator.maybeEmail, creator.name)),
              maybeDatasetParts         = dataset1.part.map(part => (part.name, part.atLocation, part.dateCreated)),
              maybeDatasetUrl           = Some(reusedDatasetUrl)
            ),
            singleFileAndCommitWithDataset(
              projectPath,
              committerEmail            = dataset2.created.agent.email,
              committerName             = dataset2.created.agent.name,
              datasetId                 = dataset2.id,
              datasetName               = dataset2.name,
              maybeDatasetDescription   = dataset2.maybeDescription,
              datasetCreatedDate        = dataset2.created.date,
              maybeDatasetPublishedDate = dataset2.published.maybeDate,
              maybeDatasetCreators      = dataset2.published.creators.map(creator => (creator.maybeEmail, creator.name)),
              maybeDatasetParts         = dataset2.part.map(part => (part.name, part.atLocation, part.dateCreated))
            )
          )
        )

        val foundDatasets = datasetsFinder.findDatasets(projectPath).unsafeRunSync()

        foundDatasets should contain theSameElementsAs List(
          dataset1.copy(part    = dataset1.part.sorted,
                        project = List(DatasetProject(projectPath), DatasetProject(otherProject)).sorted),
          dataset2.copy(part    = dataset2.part.sorted, project = List(DatasetProject(projectPath)).sorted)
        )
      }
    }

    "return None if there are no datasets in the project" in new InMemoryStoreTestCase {
      val projectPath = projectPaths.generateOne

      datasetsFinder.findDatasets(projectPath).unsafeRunSync() shouldBe List.empty
    }
  }

  private trait InMemoryStoreTestCase {
    private val logger = TestLogger[IO]()
    val datasetsFinder = new IOProjectDatasetsFinder(
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
