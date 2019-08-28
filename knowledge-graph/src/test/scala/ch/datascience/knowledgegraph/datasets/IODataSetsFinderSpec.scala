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

package ch.datascience.knowledgegraph.datasets

import DataSetsGenerators._
import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.httpUrls
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.model.{DataSetPart, DataSetProject}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.RdfStoreData._
import ch.datascience.stubbing.ExternalServiceStubbing
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODataSetsFinderSpec
    extends WordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  "findDataSets" should {

    "return all data-sets of the given project" in new InMemoryStoreTestCase {
      forAll(projectPaths, dataSets, dataSets) { (projectPath, dataSet1, dataSet2) =>
        val otherProject = projectPaths.generateOne
        val reusedDataSetUrl = (for {
          url  <- httpUrls
          uuid <- Gen.uuid
        } yield s"$url/$uuid").generateOne

        loadToStore(
          RDF(
            singleFileAndCommitWithDataset(otherProject,
                                           dataSetId       = dataSetIds.generateOne,
                                           dataSetName     = dataSetNames.generateOne,
                                           maybeDataSetUrl = Some(reusedDataSetUrl)),
            singleFileAndCommitWithDataset(
              projectPath,
              committerEmail            = dataSet1.created.agent.email,
              committerName             = dataSet1.created.agent.name,
              dataSetId                 = dataSet1.id,
              dataSetName               = dataSet1.name,
              maybeDataSetDescription   = dataSet1.maybeDescription,
              dataSetCreatedDate        = dataSet1.created.date,
              maybeDataSetPublishedDate = dataSet1.published.maybeDate,
              maybeDataSetCreators      = dataSet1.published.creators.map(creator => (creator.maybeEmail, creator.name)),
              maybeDataSetParts         = dataSet1.part.map(part => (part.name, part.atLocation, part.dateCreated)),
              maybeDataSetUrl           = Some(reusedDataSetUrl)
            ),
            singleFileAndCommitWithDataset(
              projectPath,
              committerEmail            = dataSet2.created.agent.email,
              committerName             = dataSet2.created.agent.name,
              dataSetId                 = dataSet2.id,
              dataSetName               = dataSet2.name,
              maybeDataSetDescription   = dataSet2.maybeDescription,
              dataSetCreatedDate        = dataSet2.created.date,
              maybeDataSetPublishedDate = dataSet2.published.maybeDate,
              maybeDataSetCreators      = dataSet2.published.creators.map(creator => (creator.maybeEmail, creator.name)),
              maybeDataSetParts         = dataSet2.part.map(part => (part.name, part.atLocation, part.dateCreated))
            )
          )
        )

        val foundDataSets = dataSetsFinder.findDataSets(projectPath).unsafeRunSync()

        foundDataSets should contain theSameElementsAs List(
          dataSet1.copy(part    = dataSet1.part.sorted,
                        project = List(DataSetProject(projectPath), DataSetProject(otherProject)).sorted),
          dataSet2.copy(part    = dataSet2.part.sorted, project = List(DataSetProject(projectPath)).sorted)
        )
      }
    }

    "return None if there are no data-sets in the project" in new InMemoryStoreTestCase {
      val projectPath = projectPaths.generateOne

      dataSetsFinder.findDataSets(projectPath).unsafeRunSync() shouldBe List.empty
    }
  }

  private trait InMemoryStoreTestCase {
    private val logger = TestLogger[IO]()
    val dataSetsFinder = new IODataSetsFinder(
      new BaseInfosFinder(rdfStoreConfig, renkuBaseUrl, logger),
      new CreatorsFinder(rdfStoreConfig, renkuBaseUrl, logger),
      new PartsFinder(rdfStoreConfig, renkuBaseUrl, logger),
      new ProjectsFinder(rdfStoreConfig, renkuBaseUrl, logger)
    )
  }

  private implicit lazy val partsAlphabeticalOrdering: Ordering[DataSetPart] =
    (part1: DataSetPart, part2: DataSetPart) => part1.name.value compareTo part2.name.value

  private implicit lazy val projectsAlphabeticalOrdering: Ordering[DataSetProject] =
    (project1: DataSetProject, project2: DataSetProject) => project1.name.value compareTo project2.name.value
}
