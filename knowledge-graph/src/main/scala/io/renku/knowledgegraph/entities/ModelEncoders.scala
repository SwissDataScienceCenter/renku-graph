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

package io.renku.knowledgegraph.entities

import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.entities.search.{Criteria, model}
import io.renku.graph.model.datasets.{DateCreated, DatePublished, SameAs}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.json.JsonOps._
import io.renku.knowledgegraph
import io.renku.knowledgegraph.datasets.details.RequestedDataset

object ModelEncoders extends ModelEncoders

trait ModelEncoders {

  implicit def imagesEncoder(implicit gitLabUrl: GitLabUrl): Encoder[(List[ImageUri], projects.Slug)] =
    Encoder.instance[(List[ImageUri], projects.Slug)] { case (imageUris, exemplarProjectSlug) =>
      Json.arr(imageUris.map {
        case uri: ImageUri.Relative =>
          json"""{
          "location": $uri
        }""" deepMerge _links(
            Link(Rel("view") -> Href(gitLabUrl / exemplarProjectSlug / "raw" / "master" / uri))
          )
        case uri: ImageUri.Absolute =>
          json"""{
          "location": $uri
        }""" deepMerge _links(Link(Rel("view") -> Href(uri.show)))
      }: _*)
    }

  implicit def projectEncoder(implicit renkuApiUrl: renku.ApiUrl, gitLabUrl: GitLabUrl): Encoder[model.Entity.Project] =
    Encoder.instance { project =>
      json"""{
        "type":          ${Criteria.Filters.EntityType.Project.value},
        "matchingScore": ${project.matchingScore},
        "name":          ${project.name},
        "path":          ${project.slug},
        "namespace":     ${project.slug.toNamespaces.mkString("/")},
        "namespaces":    ${toDetailedInfo(project.slug.toNamespaces)},
        "visibility":    ${project.visibility},
        "date":          ${project.date},
        "dateCreated":   ${project.date},
        "dateModified":  ${project.dateModified},
        "keywords":      ${project.keywords},
        "images":        ${(project.images -> project.slug).asJson}
      }"""
        .addIfDefined("creator" -> project.maybeCreator)
        .addIfDefined("description" -> project.maybeDescription)
        .deepMerge(
          _links(
            Link(Rel("details") -> knowledgegraph.projects.details.Endpoint.href(renkuApiUrl, project.slug))
          )
        )
    }

  private type NamespaceInfo = (Rel, List[projects.Namespace])

  private lazy val toDetailedInfo: List[projects.Namespace] => Json = _.foldLeft(List.empty[NamespaceInfo]) {
    case (Nil, namespace) => List(Rel(namespace.show) -> List(namespace))
    case (all @ (_, lastNamespaces) :: _, namespace) =>
      all ::: (Rel(namespace.show) -> (lastNamespaces ::: namespace :: Nil)) :: Nil
  }.asJson

  private implicit lazy val namespaceEncoder: Encoder[NamespaceInfo] = Encoder.instance { case (rel, namespaces) =>
    json"""{
      "rel":       $rel,
      "namespace": ${namespaces.map(_.show).mkString("/")}
    }"""
  }

  implicit def datasetEncoder(implicit
      gitLabUrl:   GitLabUrl,
      renkuApiUrl: renku.ApiUrl
  ): Encoder[model.Entity.Dataset] =
    Encoder.instance { ds =>
      json"""{
        "type":          ${Criteria.Filters.EntityType.Dataset.value},
        "matchingScore": ${ds.matchingScore},
        "name":          ${ds.name},
        "slug":          ${ds.name},
        "visibility":    ${ds.visibility},
        "date":          ${ds.date},
        "creators":      ${ds.creators},
        "keywords":      ${ds.keywords},
        "images":        ${(ds.images -> ds.exemplarProjectSlug).asJson}
      }"""
        .addIfDefined("description" -> ds.maybeDescription)
        .addIfDefined("dateModified" -> ds.dateModified)
        .deepMerge(
          ds.date match {
            case d: DateCreated   => json"""{"dateCreated":  $d }"""
            case d: DatePublished => json"""{"datePublished":  $d }"""
          }
        )
        .deepMerge(
          _links(
            Link(
              Rel("details") ->
                knowledgegraph.datasets.details.Endpoint.href(renkuApiUrl, RequestedDataset(SameAs(ds.sameAs.value)))
            )
          )
        )
    }

  implicit lazy val workflowEncoder: Encoder[model.Entity.Workflow] =
    Encoder.instance { workflow =>
      json"""{
        "type":          ${Criteria.Filters.EntityType.Workflow.value},
        "matchingScore": ${workflow.matchingScore},
        "name":          ${workflow.name},
        "visibility":    ${workflow.visibility},
        "date":          ${workflow.date},
        "keywords":      ${workflow.keywords},
        "workflowType": ${workflow.workflowType}
      }"""
        .addIfDefined("description" -> workflow.maybeDescription)
    }

  implicit lazy val personEncoder: Encoder[model.Entity.Person] =
    Encoder.instance { person =>
      json"""{
        "type":          ${Criteria.Filters.EntityType.Person.value},
        "matchingScore": ${person.matchingScore},
        "name":          ${person.name}
      }"""
    }

  implicit def modelEncoder(implicit renkuApiUrl: renku.ApiUrl, gitLabUrl: GitLabUrl): Encoder[model.Entity] =
    Encoder.instance {
      case project:  model.Entity.Project  => project.asJson
      case ds:       model.Entity.Dataset  => ds.asJson
      case workflow: model.Entity.Workflow => workflow.asJson
      case person:   model.Entity.Person   => person.asJson
    }
}
