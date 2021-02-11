/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import ch.datascience.knowledgegraph.datasets.model.{Dataset, ModifiedDataset, NonModifiedDataset}
import ch.datascience.rdfstore.entities.DataSet
import io.circe.literal._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BaseDetailsFinderSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  private implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne

  import BaseDetailsFinder._
  import io.circe.syntax._

  "non modified dataset decoder" should {

    "decode result-set with a blank description, url, sameAs, and images to a Dataset object" in {
      forAll(nonModifiedDatasets(), datasetPublishedDates, blankStrings()) { (dataset, publishedDate, description) =>
        resultSet(dataset, publishedDate, description).as[List[Dataset]] shouldBe Right {
          List(
            dataset
              .copy(published = dataset.published.copy(maybeDate = Some(publishedDate), creators = Set.empty))
              .copy(sameAs = SameAs(DataSet.entityId(dataset.id)))
              .copy(maybeDescription = None)
              .copy(parts = Nil)
              .copy(projects = Nil)
              .copy(keywords = Nil)
              .copy(images = Nil)
          )
        }
      }
    }
  }

  "modified dataset decoder" should {

    "decode result-set with a blank description, url, sameAs, and images to a Dataset object" in {
      forAll(nonModifiedDatasets(), datasetPublishedDates, blankStrings()) { (dataset, publishedDate, description) =>
        val modifiedDataset = modifiedDatasetsOnFirstProject(dataset).generateOne
        resultSet(modifiedDataset, publishedDate, description).as[List[Dataset]] shouldBe Right {
          List(
            modifiedDataset
              .copy(published = dataset.published.copy(maybeDate = Some(publishedDate), creators = Set.empty))
              .copy(maybeDescription = None)
              .copy(parts = Nil)
              .copy(projects = Nil)
              .copy(keywords = Nil)
              .copy(images = Nil)
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
          "datasetId": {"value": ${DataSet.entityId(dataset.id).value.toString}},
          "identifier": {"value": ${dataset.id.value}},
          "name": {"value": ${dataset.title.value}},
          "alternateName": {"value": ${dataset.name.value}},
          "publishedDate": {"value": ${publishedDate.value}},
          "dateCreated": {"value": ${dataset.created.value}},
          "description": {"value": $blank},
          "url": {"value": ${dataset.url.value}},
          "topmostSameAs": {"value": ${SameAs(DataSet.entityId(dataset.id)).toString} },
          "initialVersion": {"value": ${dataset.versions.initial.toString} },
          "keywords": {"value": ${dataset.keywords.map(_.value).asJson}}
        }
      ]
    }
  }"""

  private def resultSet(dataset: ModifiedDataset, publishedDate: PublishedDate, blank: String) = json"""
  {
    "results": {
      "bindings": [
        {
          "datasetId": {"value": ${DataSet.entityId(dataset.id).value.toString}},
          "identifier": {"value": ${dataset.id.value}},
          "name": {"value": ${dataset.title.value}},
          "alternateName": {"value": ${dataset.name.value}},
          "publishedDate": {"value": ${publishedDate.value}},
          "dateCreated": {"value": ${dataset.created.value}},
          "description": {"value": $blank},
          "url": {"value": ${dataset.url.value}},
          "topmostSameAs": {"value": ${DataSet.entityId(dataset.id).toString} },
          "maybeDerivedFrom": {"value": ${dataset.derivedFrom.value}},
          "initialVersion": {"value": ${dataset.versions.initial.toString} },
          "keywords": {"value": ${dataset.keywords.map(_.value).asJson}},
          "images": {"value": ${dataset.images.map(_.value).asJson}}
        }
      ]
    }
  }"""
}
