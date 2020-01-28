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

    "return all data-sets of the given project" in new InMemoryStoreTestCase {
      forAll(datasetProjects, datasets, addedToProject, datasets, addedToProject) {
        (projectB, dataset1, dataset1AddedToProjectB, dataset2, dataset2AddedToProjectB) =>
          val projectA                = datasetProjects.generateOne
          val dataset1AddedToProjectA = addedToProject.generateOne

          loadToStore(
            dataSetCommit(
              committedDate = CommittedDate(dataset1AddedToProjectA.date.value),
              committer     = Person(dataset1AddedToProjectA.agent.name, dataset1AddedToProjectA.agent.email)
            )(
              projectPath = projectA.path,
              projectName = projectA.name
            )(
              datasetIdentifier         = dataset1.id,
              datasetName               = dataset1.name,
              maybeDatasetUrl           = dataset1.maybeUrl,
              maybeDatasetSameAs        = dataset1.maybeSameAs,
              maybeDatasetDescription   = dataset1.maybeDescription,
              maybeDatasetPublishedDate = dataset1.published.maybeDate,
              datasetCreators           = dataset1.published.creators map toPerson,
              datasetParts              = dataset1.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit(
              committedDate = CommittedDate(dataset1AddedToProjectB.date.value),
              committer     = Person(dataset1AddedToProjectB.agent.name, dataset1AddedToProjectB.agent.email)
            )(
              projectPath = projectB.path,
              projectName = projectB.name
            )(
              datasetName               = dataset1.name,
              maybeDatasetUrl           = dataset1.maybeUrl,
              maybeDatasetSameAs        = dataset1.maybeSameAs,
              maybeDatasetDescription   = dataset1.maybeDescription,
              maybeDatasetPublishedDate = dataset1.published.maybeDate,
              datasetCreators           = dataset1.published.creators map toPerson,
              datasetParts              = dataset1.parts.map(part => (part.name, part.atLocation))
            ),
            dataSetCommit(
              committedDate = CommittedDate(dataset2AddedToProjectB.date.value),
              committer     = Person(dataset2AddedToProjectB.agent.name, dataset2AddedToProjectB.agent.email)
            )(
              projectPath = projectB.path,
              projectName = projectB.name
            )(
              datasetIdentifier         = dataset2.id,
              datasetName               = dataset2.name,
              maybeDatasetUrl           = dataset2.maybeUrl,
              maybeDatasetSameAs        = dataset2.maybeSameAs,
              maybeDatasetDescription   = dataset2.maybeDescription,
              maybeDatasetPublishedDate = dataset2.published.maybeDate,
              datasetCreators           = dataset2.published.creators map toPerson,
              datasetParts              = dataset2.parts.map(part => (part.name, part.atLocation))
            )
          )

          val foundDatasets = datasetsFinder.findDatasets(projectB.path).unsafeRunSync()

          foundDatasets should contain theSameElementsAs List(
            dataset1.copy(
              parts = dataset1.parts sorted byPartName,
              projects = List(
                DatasetProject(projectB.path, projectB.name, dataset1AddedToProjectB),
                DatasetProject(projectA.path, projectA.name, dataset1AddedToProjectA)
              ) sorted byProjectName
            ),
            dataset2.copy(
              parts    = dataset2.parts sorted byPartName,
              projects = List(DatasetProject(projectB.path, projectB.name, dataset2AddedToProjectB)) sorted byProjectName
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
