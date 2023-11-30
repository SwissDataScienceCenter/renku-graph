/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.graph.acceptancetests.knowledgegraph

import cats.data.NonEmptyList
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.tooling.AcceptanceSpec
import io.renku.graph.model.testentities.{Dataset, Person}
import io.renku.graph.model.{GitLabUrl, datasets, projects}
import io.renku.http.rest.Links.{Href, Rel, _links}
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.scalatest.matchers.should

trait DatasetsApiEncoders extends ImageApiEncoders {
  self: AcceptanceSpec with should.Matchers =>

  def gitLabUrl: GitLabUrl

  import io.renku.json.JsonOps._

  def briefJson(dataset:     Dataset[Dataset.Provenance.Modified],
                originalDs:  Dataset[Dataset.Provenance],
                projectSlug: projects.Slug
  )(implicit
      encoder: Encoder[(Dataset[Dataset.Provenance], Option[Dataset[Dataset.Provenance]], projects.Slug)]
  ): Json = encoder((dataset, Some(originalDs), projectSlug))

  def briefJson(dataset: Dataset[Dataset.Provenance], projectSlug: projects.Slug)(implicit
      encoder: Encoder[(Dataset[Dataset.Provenance], Option[Dataset[Dataset.Provenance]], projects.Slug)]
  ): Json = encoder((dataset, Option.empty, projectSlug))

  implicit def datasetEncoder[P <: Dataset.Provenance](implicit
      provenanceEncoder: Encoder[P]
  ): Encoder[(Dataset[P], Option[Dataset[Dataset.Provenance]], projects.Slug)] = Encoder.instance {
    case (dataset, maybeOriginalDs, projectSlug) =>
      json"""{
      "identifier": ${dataset.identification.identifier.value},
      "versions": {
        "initial": ${dataset.provenance.originalIdentifier.value}
      },
      "name":   ${dataset.identification.name.value},
      "slug":   ${dataset.identification.slug.value},
      "images": ${dataset.additionalInfo.images -> projectSlug}
    }"""
        .deepMerge(
          _links(
            Rel("details")         -> Href(renkuApiUrl / "datasets" / dataset.identification.identifier),
            Rel("initial-version") -> Href(renkuApiUrl / "datasets" / dataset.provenance.originalIdentifier),
            Rel("tags") -> Href(
              renkuApiUrl / "projects" / projectSlug / "datasets" / dataset.identification.slug / "tags"
            )
          )
        )
        .deepMerge(provenanceEncoder(dataset.provenance))
        .deepMerge(encodeMaybeDateModified(dataset.provenance))
        .deepMerge(
          maybeOriginalDs
            .map(_.provenance)
            .getOrElse(dataset.provenance)
            .date
            .asInstanceOf[datasets.CreatedOrPublished]
            .asJson
        )
        .deepDropNullValues
  }

  private implicit lazy val createdOrPublishedEncoder: Encoder[datasets.CreatedOrPublished] =
    Encoder.instance[datasets.CreatedOrPublished] {
      case d: datasets.DateCreated   => json"""{"dateCreated": $d}"""
      case d: datasets.DatePublished => json"""{"datePublished": $d}"""
    }

  private def encodeMaybeDateModified[P <: Dataset.Provenance]: P => Json = {
    case p: Dataset.Provenance.Modified => json"""{"dateModified": ${p.date}}"""
    case _ => Json.obj()
  }

  implicit def provenanceEncoder: Encoder[Dataset.Provenance] = Encoder.instance {
    case provenance: Dataset.Provenance.Modified => json"""{
        "derivedFrom": ${provenance.derivedFrom.value}
      }"""
    case provenance => json"""{
        "sameAs": ${provenance.topmostSameAs.value}
      }"""
  }

  def searchResultJson[P <: Dataset.Provenance](dataset:       Dataset[P],
                                                projectsCount: Int,
                                                projectSlug:   projects.Slug,
                                                actualResults: List[Json]
  ): Json = {
    val actualIdentifier = actualResults
      .findId(dataset.identification.name)
      .getOrElse(fail(s"No ${dataset.identification.name} dataset found among the results"))

    dataset.identification.identifier shouldBe actualIdentifier

    json"""{
      "identifier":    ${actualIdentifier.value},
      "name":          ${dataset.identification.name.value},
      "slug":          ${dataset.identification.slug.value},
      "published":     ${dataset.provenance.creators -> dataset.provenance.date},
      "date":          ${dataset.provenance.date.instant},
      "projectsCount": $projectsCount,
      "keywords":      ${dataset.additionalInfo.keywords.sorted.map(_.value)},
      "images":        ${dataset.additionalInfo.images -> projectSlug}
    }"""
      .addIfDefined("description" -> dataset.additionalInfo.maybeDescription)
      .deepMerge {
        _links(
          Rel("details") -> Href(renkuApiUrl / "datasets" / actualIdentifier)
        )
      }
  }

  private implicit def publishedEncoder[P <: Dataset.Provenance]: Encoder[(NonEmptyList[Person], P#D)] =
    Encoder.instance {
      case (creators, datasets.DatePublished(date)) => json"""{
          "creator": ${creators.toList},
          "datePublished": $date
        }"""
      case (creators, _) => json"""{
          "creator": ${creators.toList}
        }"""
    }

  private implicit lazy val personEncoder: Encoder[Person] = Encoder.instance[Person] {
    case Person(name, maybeEmail, _, _, _) => json"""{
      "name": $name
    }""" addIfDefined ("email" -> maybeEmail)
  }

  implicit class JsonsOps(jsons: List[Json]) {

    def findId(name: datasets.Name): Option[datasets.Identifier] =
      jsons
        .find(_.hcursor.downField("name").as[String].fold(throw _, _ == name.toString))
        .map(_.hcursor.downField("identifier").as[datasets.Identifier].fold(throw _, identity))
  }

  def findIdentifier(json: Json): datasets.Identifier =
    json.hcursor.downField("identifier").as[datasets.Identifier].fold(throw _, identity)
}
