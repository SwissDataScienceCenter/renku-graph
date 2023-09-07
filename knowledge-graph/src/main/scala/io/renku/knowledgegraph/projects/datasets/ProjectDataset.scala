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

package io.renku.knowledgegraph.projects.datasets

import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.graph.model.datasets._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.rest.Links.{Rel, _links}
import io.renku.knowledgegraph
import io.renku.knowledgegraph.datasets.details.RequestedDataset
import io.renku.knowledgegraph.projects.images.ImageUrisEncoder

private final case class ProjectDataset(identifier:          Identifier,
                                        originalIdentifier:  OriginalIdentifier,
                                        title:               Title,
                                        name:                Name,
                                        createdOrPublished:  CreatedOrPublished,
                                        maybeDateModified:   Option[DateModified],
                                        sameAsOrDerivedFrom: ProjectDataset.SameAsOrDerived,
                                        images:              List[ImageUri]
)

private object ProjectDataset extends ImageUrisEncoder {
  type SameAsOrDerived = Either[SameAs, DerivedFrom]

  private implicit val sameAsOrDerivedEncoder: Encoder[SameAsOrDerived] = Encoder.instance[SameAsOrDerived] {
    case Left(sameAs: SameAs)            => json"""{"sameAs": $sameAs}"""
    case Right(derivedFrom: DerivedFrom) => json"""{"derivedFrom": $derivedFrom}"""
  }

  private implicit val createdOrPublishedEncoder: Encoder[CreatedOrPublished] = Encoder.instance[CreatedOrPublished] {
    case d: DateCreated   => json"""{"dateCreated": $d}"""
    case d: DatePublished => json"""{"datePublished": $d}"""
  }

  private implicit val maybeDateModifiedEncoder: Encoder[(SameAsOrDerived, Option[DateModified])] =
    Encoder.instance[(SameAsOrDerived, Option[DateModified])] {
      case (Right(_), Some(dateModified)) => json"""{"dateModified": $dateModified}"""
      case _                              => Json.obj()
    }

  def encoder(
      projectSlug: projects.Slug
  )(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl): Encoder[ProjectDataset] =
    Encoder.instance[ProjectDataset] {
      case ProjectDataset(id,
                          originalId,
                          title,
                          name,
                          createdOrPublished,
                          maybeDateModified,
                          sameAsOrDerived,
                          images
          ) =>
        json"""{
        "identifier": $id,
        "versions": {
          "initial": $originalId
        },
        "title":  $title,
        "name":   $name,
        "slug":   $name,
        "images": ${images -> projectSlug}
      }"""
          .deepMerge(sameAsOrDerived.asJson)
          .deepMerge(createdOrPublished.asJson)
          .deepMerge((sameAsOrDerived -> maybeDateModified).asJson)
          .deepMerge(
            _links(
              Rel("details") -> knowledgegraph.datasets.details.Endpoint.href(renkuApiUrl, RequestedDataset(id)),
              Rel("initial-version") ->
                knowledgegraph.datasets.details.Endpoint.href(renkuApiUrl, RequestedDataset(originalId.asIdentifier)),
              Rel("tags") -> knowledgegraph.projects.datasets.tags.Endpoint.href(renkuApiUrl, projectSlug, name)
            )
          )
          .deepDropNullValues
    }
}
