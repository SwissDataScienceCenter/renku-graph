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

package ch.datascience.knowledgegraph.datasets.graphql

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{DatasetCreator, DatasetPart, DatasetProject}
import ch.datascience.knowledgegraph.datasets.{CreatorsFinder, PartsFinder, ProjectsFinder}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.stubbing.ExternalServiceStubbing
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
      forAll(datasetProjects, datasets, datasetInProjectCreations, datasets, datasetInProjectCreations) {
        (projectB, dataset1, projectBDataset1Creation, dataset2, projectBDataset2Creation) =>
          val projectA         = datasetProjects.generateOne
          val projectACreation = datasetInProjectCreations.generateOne

          loadToStore(
            dataSetCommit(
              committedDate = CommittedDate(projectACreation.date.value),
              committer     = Person(projectACreation.agent.name, projectACreation.agent.email)
            )(
              projectPath = projectA.path,
              projectName = projectA.name
            )(
              dataset1.id,
              dataset1.name,
              dataset1.maybeUrl,
              dataset1.maybeSameAs,
              dataset1.maybeDescription,
              dataset1.published.maybeDate,
              datasetCreators = dataset1.published.creators map toPerson,
              datasetParts    = dataset1.part.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit(
              committedDate = CommittedDate(projectBDataset1Creation.date.value),
              committer     = Person(projectBDataset1Creation.agent.name, projectBDataset1Creation.agent.email)
            )(
              projectPath = projectB.path,
              projectName = projectB.name
            )(
              dataset1.id,
              dataset1.name,
              dataset1.maybeUrl,
              dataset1.maybeSameAs,
              dataset1.maybeDescription,
              dataset1.published.maybeDate,
              datasetCreators = dataset1.published.creators map toPerson,
              datasetParts    = dataset1.part.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit(
              committedDate = CommittedDate(projectBDataset2Creation.date.value),
              committer     = Person(projectBDataset2Creation.agent.name, projectBDataset2Creation.agent.email)
            )(
              projectPath = projectB.path,
              projectName = projectB.name
            )(
              dataset2.id,
              dataset2.name,
              dataset2.maybeUrl,
              dataset2.maybeSameAs,
              dataset2.maybeDescription,
              dataset2.published.maybeDate,
              datasetCreators = dataset2.published.creators map toPerson,
              datasetParts    = dataset2.part.map(part => (part.name, part.atLocation))
            )
          )

          val foundDatasets = datasetsFinder.findDatasets(projectB.path).unsafeRunSync()

          foundDatasets should contain theSameElementsAs List(
            dataset1.copy(
              part = dataset1.part sorted byPartName,
              project = List(DatasetProject(projectB.path, projectB.name, projectBDataset1Creation),
                             DatasetProject(projectA.path, projectA.name, projectACreation)) sorted byProjectName
            ),
            dataset2.copy(
              part    = dataset2.part sorted byPartName,
              project = List(DatasetProject(projectB.path, projectB.name, projectBDataset2Creation)) sorted byProjectName
            )
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

  private lazy val toPerson: DatasetCreator => Person =
    creator => Person(creator.name, creator.maybeEmail, creator.maybeAffiliation)

  private lazy val byPartName: Ordering[DatasetPart] =
    (part1: DatasetPart, part2: DatasetPart) => part1.name.value compareTo part2.name.value

  private lazy val byProjectName: Ordering[DatasetProject] =
    (project1: DatasetProject, project2: DatasetProject) => project1.name.value compareTo project2.name.value
}
