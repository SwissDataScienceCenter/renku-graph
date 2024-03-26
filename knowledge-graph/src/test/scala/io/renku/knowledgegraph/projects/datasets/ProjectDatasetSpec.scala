/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.datasets

import Generators._
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.config.renku
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.client.{GitLabGenerators, GitLabUrl}
import io.renku.knowledgegraph.projects.images.ImageUrisEncoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectDatasetSpec extends AnyFlatSpec with should.Matchers with ScalaCheckPropertyChecks with ImageUrisEncoder {

  it should "encode to JSON" in {
    forAll(projectDatasetGen) { datasets =>
      datasets.asJson(ProjectDataset.encoder(projectSlug)) shouldBe toJson(datasets)
    }
  }

  private lazy val toJson: ProjectDataset => Json = {
    case ProjectDataset(id, originalId, name, slug, createdOrPublished, _, Left(sameAs), images) =>
      json"""{
        "identifier": $id,
        "versions": {
          "initial": $originalId
        },
        "name":          $name,
        "slug":          $slug,
        "dateCreated":   ${createdOrPublished.fold(_.asJson, _ => Json.Null)},
        "datePublished": ${createdOrPublished.fold(_ => Json.Null, _.asJson)},
        "sameAs":        $sameAs,
        "images":        ${images -> projectSlug},
        "_links": [{
          "rel":  "details",
          "href": ${renkuApiUrl / "datasets" / id}
        }, {
          "rel":  "initial-version",
          "href": ${renkuApiUrl / "datasets" / originalId}
        }, {
          "rel":  "tags",
          "href": ${renkuApiUrl / "projects" / projectSlug / "datasets" / slug / "tags"}
        }]
      }""".deepDropNullValues
    case ProjectDataset(id,
                        originalId,
                        name,
                        slug,
                        createdOrPublished,
                        Some(dateModified),
                        Right(derivedFrom),
                        images
        ) =>
      json"""{
        "identifier": $id,
        "versions" : {
          "initial": $originalId
        },
        "name":          $name,
        "slug":          $slug,
        "dateCreated":   ${createdOrPublished.fold(_.asJson, _ => Json.Null)},
        "datePublished": ${createdOrPublished.fold(_ => Json.Null, _.asJson)},
        "dateModified":  $dateModified,
        "derivedFrom":   $derivedFrom,
        "images":        ${images -> projectSlug},
        "_links": [{
          "rel":  "details",
          "href": ${renkuApiUrl / "datasets" / id}
        }, {
          "rel":  "initial-version",
          "href": ${renkuApiUrl / "datasets" / originalId}
        }, {
          "rel":  "tags",
          "href": ${renkuApiUrl / "projects" / projectSlug / "datasets" / slug / "tags"}
        }]
      }""".deepDropNullValues
    case other => fail(s"Invalid ProjectDataset $other")
  }

  private lazy val projectSlug = projectSlugs.generateOne
  private implicit lazy val renkuApiUrl: renku.ApiUrl = renkuApiUrls.generateOne
  private implicit lazy val gitLabUrl:   GitLabUrl    = GitLabGenerators.gitLabUrls.generateOne
}
