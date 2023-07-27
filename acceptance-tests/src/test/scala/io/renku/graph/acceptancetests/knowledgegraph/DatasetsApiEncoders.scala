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

  def briefJson(dataset: Dataset[Dataset.Provenance], projectPath: projects.Path)(implicit
      encoder: Encoder[(Dataset[Dataset.Provenance], projects.Path)]
  ): Json = encoder(dataset -> projectPath)

  implicit def datasetEncoder[P <: Dataset.Provenance](implicit
      provenanceEncoder: Encoder[P]
  ): Encoder[(Dataset[P], projects.Path)] = Encoder.instance { case (dataset, projectPath) =>
    json"""{
      "identifier": ${dataset.identification.identifier.value},
      "versions": {
        "initial": ${dataset.provenance.originalIdentifier.value}
      },
      "title":  ${dataset.identification.title.value},
      "name":   ${dataset.identification.name.value},
      "slug":   ${dataset.identification.name.value},
      "images": ${dataset.additionalInfo.images -> projectPath}
    }"""
      .deepMerge(
        _links(
          Rel("details")         -> Href(renkuApiUrl / "datasets" / dataset.identification.identifier),
          Rel("initial-version") -> Href(renkuApiUrl / "datasets" / dataset.provenance.originalIdentifier),
          Rel("tags") -> Href(
            renkuApiUrl / "projects" / projectPath / "datasets" / dataset.identification.name / "tags"
          )
        )
      )
      .deepMerge(provenanceEncoder(dataset.provenance))
      .deepMerge(dataset.provenance.date.asInstanceOf[datasets.CreatedOrPublished].asJson)
      .deepMerge(encodeMaybeDateModified(dataset.provenance))
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
                                                projectPath:   projects.Path,
                                                actualResults: List[Json]
  ): Json = {
    val actualIdentifier = actualResults
      .findId(dataset.identification.title)
      .getOrElse(fail(s"No ${dataset.identification.title} dataset found among the results"))

    dataset.identification.identifier shouldBe actualIdentifier

    json"""{
      "identifier":    ${actualIdentifier.value},
      "title":         ${dataset.identification.title.value},
      "name":          ${dataset.identification.name.value},
      "slug":          ${dataset.identification.name.value},
      "published":     ${dataset.provenance.creators -> dataset.provenance.date},
      "date":          ${dataset.provenance.date.instant},
      "projectsCount": $projectsCount,
      "keywords":      ${dataset.additionalInfo.keywords.sorted.map(_.value)},
      "images":        ${dataset.additionalInfo.images -> projectPath}
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

    def findId(title: datasets.Title): Option[datasets.Identifier] =
      jsons
        .find(_.hcursor.downField("title").as[String].fold(throw _, _ == title.toString))
        .map(_.hcursor.downField("identifier").as[datasets.Identifier].fold(throw _, identity))
  }

  def findIdentifier(json: Json): datasets.Identifier =
    json.hcursor.downField("identifier").as[datasets.Identifier].fold(throw _, identity)
}
