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

    "return the details of the dataset with the given id" in new InMemoryStoreTestCase {
      forAll(projectPaths, datasets, datasetInProjectCreations) { (projectPath, dataset1, dataset1Creation) =>
        val otherProject         = projectPaths.generateOne
        val otherProjectCreation = datasetInProjectCreations.generateOne
        val reusedDatasetUrl = (for {
          url  <- httpUrls
          uuid <- Gen.uuid
        } yield s"$url/$uuid").generateOne

        loadToStore(
          triples(
            singleFileAndCommitWithDataset(
              otherProject,
              committerName     = otherProjectCreation.agent.name,
              committerEmail    = otherProjectCreation.agent.email,
              committedDate     = otherProjectCreation.date.toUnsafe(date => CommittedDate.from(date.value)),
              datasetIdentifier = datasetIds.generateOne,
              datasetName       = datasetNames.generateOne,
              maybeDatasetUrl   = Some(reusedDatasetUrl)
            ),
            singleFileAndCommitWithDataset(
              projectPath,
              committerName             = dataset1Creation.agent.name,
              committerEmail            = dataset1Creation.agent.email,
              committedDate             = dataset1Creation.date.toUnsafe(date => CommittedDate.from(date.value)),
              datasetIdentifier         = dataset1.id,
              datasetName               = dataset1.name,
              maybeDatasetDescription   = dataset1.maybeDescription,
              maybeDatasetPublishedDate = dataset1.published.maybeDate,
              maybeDatasetCreators      = dataset1.published.creators.map(creator => (creator.name, creator.maybeEmail)),
              maybeDatasetParts         = dataset1.part.map(part => (part.name, part.atLocation)),
              maybeDatasetUrl           = Some(reusedDatasetUrl)
            ),
            singleFileAndCommitWithDataset(projectPath)
          )
        )

        datasetFinder.findDataset(dataset1.id).unsafeRunSync() shouldBe Some(
          dataset1.copy(part = dataset1.part.sorted,
                        project = List(DatasetProject(projectPath, dataset1Creation),
                                       DatasetProject(otherProject, otherProjectCreation)).sorted)
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
