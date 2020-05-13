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

import ch.datascience.generators.CommonGraphGenerators.renkuBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{PublishedDate, SameAs}
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{ModifiedDataset, NonModifiedDataset}
import ch.datascience.rdfstore.entities.DataSet
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BaseDetailsFinderSpec extends WordSpec with ScalaCheckPropertyChecks {

  private implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne

  import BaseDetailsFinder._

  "non modified dataset decoder" should {

    "decode result-set with a blank description, url, and sameAs to a Dataset object" in {
      forAll(nonModifiedDatasets(), datasetPublishedDates, blankStrings()) { (dataset, publishedDate, description) =>
        resultSet(dataset, publishedDate, description).as[List[NonModifiedDataset]] shouldBe Right {
          List(
            dataset
              .copy(published = dataset.published.copy(maybeDate = Some(publishedDate), creators = Set.empty))
              .copy(sameAs = SameAs(DataSet.entityId(dataset.id).value))
              .copy(maybeDescription = None)
              .copy(parts = Nil)
              .copy(projects = Nil)
          )
        }
      }
    }
  }

  "modified dataset decoder" should {

    "decode result-set with a blank description, url, and sameAs to a Dataset object" in {
      forAll(modifiedDatasets(), datasetPublishedDates, blankStrings()) { (dataset, publishedDate, description) =>
        resultSet(dataset, publishedDate, description).as[List[ModifiedDataset]] shouldBe Right {
          List(
            dataset
              .copy(published = dataset.published.copy(maybeDate = Some(publishedDate), creators = Set.empty))
              .copy(maybeDescription = None)
              .copy(parts = Nil)
              .copy(projects = Nil)
          )
        }
      }
    }
  }

  private def resultSet(dataset: NonModifiedDataset, publishedDate: PublishedDate, blank: String) = json"""
  {
    "results": {
      "bindings": [
        {
          "datasetId": {"value": ${DataSet.entityId(dataset.id).value}},
          "identifier": {"value": ${dataset.id.value}},
          "name": {"value": ${dataset.name.value}},
          "publishedDate": {"value": ${publishedDate.value}},
          "description": {"value": $blank},
          "url": {"value": ${dataset.url.value}},
          "sameAs": {"value": $blank}
        }
      ]
    }
  }"""

  private def resultSet(dataset: ModifiedDataset, publishedDate: PublishedDate, blank: String) = json"""
  {
    "results": {
      "bindings": [
        {
          "datasetId": {"value": ${DataSet.entityId(dataset.id).value}},
          "identifier": {"value": ${dataset.id.value}},
          "name": {"value": ${dataset.name.value}},
          "publishedDate": {"value": ${publishedDate.value}},
          "description": {"value": $blank},
          "url": {"value": ${dataset.url.value}},
          "derivedFrom": {"value": ${dataset.derivedFrom.value}}
        }
      ]
    }
  }"""
}
