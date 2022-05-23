/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.datasets.rest

import cats.syntax.all._
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.datasets.ResourceId
import io.renku.graph.model.testentities._
import io.renku.graph.model.{RenkuBaseUrl, datasets, testentities}
import io.renku.jsonld.syntax._
import io.renku.knowledgegraph.datasets.model
import io.renku.tinytypes.json.TinyTypeEncoders
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BaseDetailsFinderSpec
    extends AnyWordSpec
    with ScalaCheckPropertyChecks
    with should.Matchers
    with TinyTypeEncoders {

  import BaseDetailsFinderImpl._

  "non-modified dataset decoder" should {

    "decode result-set with a blank description, url, sameAs, and images to a Dataset object" in {
      Set(
        anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceInternal))
          .map { case (ds, project) => (ds, project, internalToNonModified(ds, project)) }
          .generateOne,
        anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedExternal))
          .map { case (ds, project) => (ds, project, importedExternalToNonModified(ds, project)) }
          .generateOne,
        anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedInternalAncestorInternal()))
          .map { case (ds, project) => (ds, project, importedInternalToNonModified(ds, project)) }
          .generateOne,
        anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedInternalAncestorExternal))
          .map { case (ds, project) => (ds, project, importedInternalToNonModified(ds, project)) }
          .generateOne
      ) foreach { case (dataset, project, nonModifiedDataset) =>
        nonModifiedToResultSet(project, dataset, blankStrings().generateOne)
          .as[Option[model.Dataset]](maybeDatasetDecoder(dataset.identification.identifier)) shouldBe
          nonModifiedDataset
            .copy(creators = List.empty)
            .copy(maybeDescription = None)
            .copy(parts = Nil)
            .copy(usedIn = Nil)
            .copy(keywords = Nil)
            .copy(images = Nil)
            .some
            .asRight
      }
    }
  }

  "modified dataset decoder" should {

    "decode result-set with a blank description, url, sameAs, and images to a Dataset object" in {
      forAll(
        anyRenkuProjectEntities.addDatasetAndModification(datasetEntities(provenanceNonModified)),
        blankStrings()
      ) { case ((_ ::~ dataset, project), description) =>
        modifiedToResultSet(project, dataset, description)
          .as[Option[model.Dataset]](maybeDatasetDecoder(dataset.identification.identifier)) shouldBe
          modifiedToModified(dataset, project)
            .copy(creators = List.empty)
            .copy(maybeDescription = None)
            .copy(parts = Nil)
            .copy(usedIn = Nil)
            .copy(keywords = Nil)
            .copy(images = Nil)
            .some
            .asRight
      }
    }
  }

  private def nonModifiedToResultSet(project:     testentities.RenkuProject,
                                     dataset:     testentities.Dataset[testentities.Dataset.Provenance.NonModified],
                                     description: String
  )(implicit renkuBaseUrl:                        RenkuBaseUrl) = {
    val binding = json"""{
      "datasetId":      {"value": ${ResourceId(dataset.asEntityId.show)}},
      "identifier":     {"value": ${dataset.identifier}},
      "name":           {"value": ${dataset.identification.title}},
      "slug":           {"value": ${dataset.identification.name}},
      "description":    {"value": $description},
      "topmostSameAs":  {"value": ${dataset.provenance.topmostSameAs}},
      "initialVersion": {"value": ${dataset.provenance.originalIdentifier}},
      "projectPath":    {"value": ${project.path}},
      "projectName":    {"value": ${project.name}}
    }""" deepMerge {
      dataset.provenance.date match {
        case date: datasets.DatePublished => json"""{
          "maybeDatePublished": {"value": $date}
        }"""
        case date: datasets.DateCreated => json"""{
          "maybeDateCreated": {"value": $date}
        }"""
      }
    }

    json"""{"results": {"bindings": [$binding]}}"""
  }

  private def modifiedToResultSet(project:     testentities.RenkuProject,
                                  dataset:     testentities.Dataset[testentities.Dataset.Provenance.Modified],
                                  description: String
  ) = {
    val binding = json"""{
      "datasetId":        {"value": ${ResourceId(dataset.asEntityId.show)}},
      "identifier":       {"value": ${dataset.identifier}},
      "name":             {"value": ${dataset.identification.title}},
      "slug":             {"value": ${dataset.identification.name}},
      "description":      {"value": $description},
      "topmostSameAs":    {"value": ${dataset.provenance.topmostSameAs}},
      "maybeDerivedFrom": {"value": ${dataset.provenance.derivedFrom}},
      "maybeDateCreated": {"value": ${dataset.provenance.date}},
      "initialVersion":   {"value": ${dataset.provenance.originalIdentifier}},
      "projectPath":      {"value": ${project.path}},
      "projectName":      {"value": ${project.name}}
    }"""

    json"""{"results": {"bindings": [$binding]}}"""
  }
}
