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

package ch.datascience.knowledgegraph.graphql.datasets

import DataSetsGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.RdfStoreData._
import ch.datascience.rdfstore.{InMemoryRdfStore, RdfStoreData}
import ch.datascience.stubbing.ExternalServiceStubbing
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
        loadToStore(
          RDF(
            singleFileAndCommitWithDataset(projectPaths.generateOne,
                                           dataSetId   = dataSetIds.generateOne,
                                           dataSetName = dataSetNames.generateOne),
            singleFileAndCommitWithDataset(
              projectPath,
              dataSetId                 = dataSet1.id,
              dataSetName               = dataSet1.name,
              dataSetCreatedDate        = dataSet1.created.date,
              dataSetCreatorEmail       = dataSet1.created.creator.email,
              dataSetCreatorName        = dataSet1.created.creator.name,
              maybeDataSetPublishedDate = dataSet1.maybePublished.map(_.date)
            ),
            singleFileAndCommitWithDataset(
              projectPath,
              dataSetId                 = dataSet2.id,
              dataSetName               = dataSet2.name,
              dataSetCreatedDate        = dataSet2.created.date,
              dataSetCreatorEmail       = dataSet2.created.creator.email,
              dataSetCreatorName        = dataSet2.created.creator.name,
              maybeDataSetPublishedDate = dataSet2.maybePublished.map(_.date)
            )
          )
        )

        val foundDataSets = dataSetsFinder.findDataSets(projectPath).unsafeRunSync()

        foundDataSets should contain theSameElementsAs List(dataSet1, dataSet2)
      }
    }

    "return None if there are no data-sets in the project" in new InMemoryStoreTestCase {
      val projectPath = projectPaths.generateOne

      dataSetsFinder.findDataSets(projectPath).unsafeRunSync() shouldBe List.empty
    }
  }

  private trait InMemoryStoreTestCase {
    val dataSetsFinder = new IODataSetsFinder(rdfStoreConfig, RdfStoreData.renkuBaseUrl, TestLogger())
  }
}
