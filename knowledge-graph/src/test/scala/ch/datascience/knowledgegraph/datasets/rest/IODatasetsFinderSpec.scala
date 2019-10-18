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
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.datasets.{Description, Name}
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators.datasets
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.DatasetSearchResult
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples.{singleFileAndCommitWithDataset, triples}
import ch.datascience.stubbing.ExternalServiceStubbing
import eu.timepit.refined.api.Refined
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetsFinderSpec
    extends WordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  "findDatasets" should {

    "return datasets with name, description or creator matching the given phrase" in new TestCase {
      forAll(datasets, datasets, datasets) { (dataset1Orig, dataset2Orig, dataset3) =>
        val phrase = phrases.generateOne
        val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
        val dataset1 = dataset1Orig.copy(
          name = sentenceContaining(nonEmptyPhrase).map(_.value).map(Name.apply).generateOne
        )
        val dataset2 = dataset2Orig.copy(
          maybeDescription = Some(sentenceContaining(nonEmptyPhrase).map(_.value).map(Description.apply).generateOne)
        )

        loadToStore(
          triples(
            singleFileAndCommitWithDataset(
              projectPaths.generateOne,
              datasetIdentifier       = dataset1.id,
              datasetName             = dataset1.name,
              maybeDatasetDescription = dataset1.maybeDescription
            ),
            singleFileAndCommitWithDataset(
              projectPaths.generateOne,
              datasetIdentifier       = dataset2.id,
              datasetName             = dataset2.name,
              maybeDatasetDescription = dataset2.maybeDescription
            ),
            singleFileAndCommitWithDataset(
              projectPaths.generateOne,
              datasetIdentifier       = dataset3.id,
              datasetName             = dataset3.name,
              maybeDatasetDescription = dataset3.maybeDescription,
              maybeDatasetCreators = Set(
                sentenceContaining(nonEmptyPhrase).map(_.value).map(UserName.apply).generateOne -> Gen
                  .option(emails)
                  .generateOne
              )
            ),
            singleFileAndCommitWithDataset(projectPaths.generateOne),
          )
        )

        datasetsFinder
          .findDatasets(phrase)
          .unsafeRunSync() should contain theSameElementsAs List(
          DatasetSearchResult(dataset1.id, dataset1.name, dataset1.maybeDescription),
          DatasetSearchResult(dataset2.id, dataset2.name, dataset2.maybeDescription),
          DatasetSearchResult(dataset3.id, dataset3.name, dataset3.maybeDescription)
        )
      }
    }

    "return no results if there's no datasets with name, description or creator matching the given phrase" in new TestCase {

      loadToStore(
        triples(
          singleFileAndCommitWithDataset(projectPaths.generateOne),
        )
      )

      datasetsFinder
        .findDatasets(phrases.generateOne)
        .unsafeRunSync() shouldBe empty
    }
  }

  private trait TestCase {
    val datasetsFinder = new IODatasetsFinder(rdfStoreConfig, TestLogger[IO]())
  }
}
