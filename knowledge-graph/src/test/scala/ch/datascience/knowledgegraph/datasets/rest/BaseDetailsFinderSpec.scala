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

import ch.datascience.generators.CommonGraphGenerators.{renkuBaseUrls, renkuResourcesUrls}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.SameAs
import ch.datascience.graph.model.{datasets, projects}
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.rdfstore.entities
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.literal._
import io.circe.{Encoder, Json}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BaseDetailsFinderSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  private implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne

  import BaseDetailsFinder._
  import io.circe.syntax._

  "non modified dataset decoder" should {

    "decode result-set with a blank description, url, sameAs, and images to a Dataset object" in {
      forAll(nonModifiedDatasets(), blankStrings()) { (dataset, description) =>
        resultSet(dataset, description).as[List[Dataset]](datasetsDecoder(List(dataset.project))) shouldBe Right {
          List(
            dataset
              .copy(sameAs = SameAs(entities.Dataset.entityId(dataset.id)))
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
      forAll(nonModifiedDatasets(), blankStrings()) { (dataset, description) =>
        val modifiedDataset = modifiedDatasetsOnFirstProject(dataset).generateOne
        resultSet(modifiedDataset, description).as[List[Dataset]](
          datasetsDecoder(List(dataset.project))
        ) shouldBe Right {
          List(
            modifiedDataset
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

  private def resultSet(dataset: NonModifiedDataset, blank: String) = {
    val binding = json"""{
      "datasetId": {"value": ${entities.Dataset.entityId(dataset.id).value.toString}},
      "identifier": {"value": ${dataset.id.value}},
      "name": {"value": ${dataset.title.value}},
      "alternateName": {"value": ${dataset.name.value}},
      "description": {"value": $blank},
      "url": {"value": ${dataset.url.value}},
      "topmostSameAs": {"value": ${datasets.SameAs(entities.Dataset.entityId(dataset.id)).toString}},
      "initialVersion": {"value": ${dataset.versions.initial.toString}},
      "keywords": {"value": ${dataset.keywords.map(_.value).asJson}},
      "projectId": {"value": ${projects.ResourceId.apply(renkuBaseUrl, dataset.project.path).asJson}}
    }""".addDates(dataset.date)

    json"""{"results": {"bindings": [$binding]}}"""
  }

  private def resultSet(dataset: ModifiedDataset, blank: String) = {
    val binding = json"""{
      "datasetId": {"value": ${entities.Dataset.entityId(dataset.id).value.toString}},
      "identifier": {"value": ${dataset.id.value}},
      "name": {"value": ${dataset.title.value}},
      "alternateName": {"value": ${dataset.name.value}},
      "description": {"value": $blank},
      "url": {"value": ${dataset.url.value}},
      "topmostSameAs": {"value": ${entities.Dataset.entityId(dataset.id).toString} },
      "maybeDerivedFrom": {"value": ${dataset.derivedFrom.value}},
      "initialVersion": {"value": ${dataset.versions.initial.toString} },
      "keywords": {"value": ${dataset.keywords.map(_.value).asJson}},
      "images": {"value": ${dataset.images.map(_.value).asJson}},
      "projectId": {"value": ${projects.ResourceId.apply(renkuBaseUrl, dataset.project.path).asJson}}
    }""".addDates(dataset.date)

    json"""{"results": {"bindings": [$binding]}}"""
  }

  private implicit class JsonOps(json: Json) {

    lazy val addDates: datasets.Date => Json = {
      case datasets.DatePublished(published) =>
        json.deepMerge(json"""{"maybePublishedDate": {"value": $published}}""")
      case datasets.DateCreated(created) =>
        json.deepMerge(json"""{"maybeDateCreated": {"value": $created}}""")
    }
  }

  private implicit lazy val projectEncoder: Encoder[DatasetProject] = Encoder.instance[DatasetProject] { project =>
    json"""{
      "path": ${project.path.value},
      "name": ${project.name.value}
    }""" deepMerge _links(
      Link(
        Rel("project-details") -> Href(renkuResourcesUrls.generateOne / "projects" / project.path)
      )
    )
  }
}
