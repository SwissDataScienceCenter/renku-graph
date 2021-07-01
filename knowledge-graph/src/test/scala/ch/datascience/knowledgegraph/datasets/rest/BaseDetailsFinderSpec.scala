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

import ch.datascience.generators.Generators._
import ch.datascience.graph.model.testentities
import ch.datascience.graph.model.testentities._
import ch.datascience.knowledgegraph.datasets.model
import ch.datascience.knowledgegraph.datasets.model._
import io.circe.literal._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BaseDetailsFinderSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  import BaseDetailsFinder._

  "non modified dataset decoder" should {

    "decode result-set with a blank description, url, sameAs, and images to a Dataset object" in {
      forAll(
        datasetEntities(datasetProvenanceImportedExternal),
        blankStrings()
      ) { (dataset, description) =>
        nonModifiedToResultSet(dataset, description).as[List[model.Dataset]](datasetsDecoder) shouldBe Right {
          List(
            dataset
              .to[NonModifiedDataset]
              .copy(creators = Set.empty)
              .copy(maybeDescription = None)
              .copy(parts = Nil)
              .copy(usedIn = Nil)
              .copy(keywords = Nil)
              .copy(images = Nil)
          )
        }
      }
    }
  }

  "modified dataset decoder" should {

    "decode result-set with a blank description, url, sameAs, and images to a Dataset object" in {
      forAll(
        datasetEntities(datasetProvenanceModified),
        blankStrings()
      ) { (dataset, description) =>
        modifiedToResultSet(dataset, description).as[List[model.Dataset]](datasetsDecoder) shouldBe Right {
          List(
            dataset
              .to[ModifiedDataset]
              .copy(creators = Set.empty)
              .copy(maybeDescription = None)
              .copy(parts = Nil)
              .copy(usedIn = Nil)
              .copy(keywords = Nil)
              .copy(images = Nil)
          )
        }
      }
    }
  }

  private def nonModifiedToResultSet(dataset:     testentities.Dataset[testentities.Dataset.Provenance.ImportedExternal],
                                     description: String
  ) = {
    val binding = json"""{
      "identifier":         {"value": ${dataset.identifier.value}},
      "name":               {"value": ${dataset.identification.title.value}},
      "alternateName":      {"value": ${dataset.identification.name.value}},
      "url":                {"value": ${dataset.additionalInfo.url.value}},
      "description":        {"value": $description},
      "topmostSameAs":      {"value": ${dataset.provenance.topmostSameAs.value}},
      "maybeDatePublished": {"value": ${dataset.provenance.date.value}},
      "initialVersion":     {"value": ${dataset.provenance.initialVersion.value}},
      "projectId":          {"value": ${dataset.project.asEntityId.asJson}},
      "projectName":        {"value": ${dataset.project.name.value}}
    }"""

    json"""{"results": {"bindings": [$binding]}}"""
  }

  private def modifiedToResultSet(dataset:     testentities.Dataset[testentities.Dataset.Provenance.Modified],
                                  description: String
  ) = {
    val binding = json"""{
      "identifier":       {"value": ${dataset.identifier.value}},
      "name":             {"value": ${dataset.identification.title.value}},
      "alternateName":    {"value": ${dataset.identification.name.value}},
      "url":              {"value": ${dataset.additionalInfo.url.value}},
      "description":      {"value": $description},
      "topmostSameAs":    {"value": ${dataset.provenance.topmostSameAs.value} },
      "maybeDerivedFrom": {"value": ${dataset.provenance.derivedFrom.value}},
      "maybeDateCreated": {"value": ${dataset.provenance.date.value}},
      "initialVersion":   {"value": ${dataset.provenance.initialVersion.value} },
      "projectId":        {"value": ${dataset.project.asEntityId.asJson}},
      "projectName":      {"value": ${dataset.project.name.value}}
    }"""

    json"""{"results": {"bindings": [$binding]}}"""
  }
}
