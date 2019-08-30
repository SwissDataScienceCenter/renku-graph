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
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DataSetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{DataSetPart, DataSetProject}
import ch.datascience.knowledgegraph.datasets.{CreatorsFinder, PartsFinder, ProjectsFinder}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.RdfStoreData._
import ch.datascience.stubbing.ExternalServiceStubbing
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODataSetFinderSpec
    extends WordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  "findDataSet" should {

    "return the details of the data-set with the given id" in new InMemoryStoreTestCase {
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
            singleFileAndCommitWithDataset(projectPath)
          )
        )

        dataSetFinder.findDataSet(dataSet1.id).unsafeRunSync() shouldBe Some(
          dataSet1.copy(part    = dataSet1.part.sorted,
                        project = List(DataSetProject(projectPath), DataSetProject(otherProject)).sorted)
        )
      }
    }

    "return None if there's no data-sets with the given id" in new InMemoryStoreTestCase {
      val identifier = dataSetIds.generateOne
      dataSetFinder.findDataSet(identifier).unsafeRunSync() shouldBe None
    }
  }

  private trait InMemoryStoreTestCase {
    private val logger = TestLogger[IO]()
    val dataSetFinder = new IODataSetFinder(
      new BaseDetailsFinder(rdfStoreConfig, renkuBaseUrl, logger),
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
