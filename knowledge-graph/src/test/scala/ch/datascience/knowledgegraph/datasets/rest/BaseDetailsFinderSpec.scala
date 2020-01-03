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

import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.PublishedDate
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.Dataset
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BaseDetailsFinderSpec extends WordSpec with ScalaCheckPropertyChecks {

  import BaseDetailsFinder._

  "dataset decoder" should {

    "decode result-set with a blank description, url, and sameAs to a Dataset object" in {
      forAll(datasets, datasetPublishedDates, blankStrings()) { (dataset, publishedDate, description) =>
        resultSet(dataset, publishedDate, description).as[List[Dataset]] shouldBe Right {
          List(
            dataset
              .copy(published = dataset.published.copy(maybeDate = Some(publishedDate), creators = Set.empty))
              .copy(maybeUrl = None)
              .copy(maybeSameAs = None)
              .copy(maybeDescription = None)
              .copy(part = Nil)
              .copy(project = Nil)
          )
        }
      }
    }
  }

  private def resultSet(dataset: Dataset, publishedDate: PublishedDate, blank: String) = json"""
  {
    "results": {
      "bindings": [
        {
          "identifier": {"value": ${dataset.id.value}},
          "name": {"value": ${dataset.name.value}},
          "publishedDate": {"value": ${publishedDate.value}},
          "description": {"value": $blank},
          "url": {"value": $blank},
          "sameAs": {"value": $blank}
        }
      ]
    }
  }"""
}
