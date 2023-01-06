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
import io.circe.Encoder
import io.renku.config.renku
import io.renku.graph.model.datasets.{DerivedFrom, SameAs}

import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.rest.Links.{Rel, _links}
import io.renku.knowledgegraph
import io.renku.knowledgegraph.projects.images.ImagesEncoder

private object ProjectDatasetEncoder extends ImagesEncoder {

  import ProjectDatasetsFinder._

  private implicit val sameAsOrDerivedEncoder: Encoder[SameAsOrDerived] = Encoder.instance[SameAsOrDerived] {
    case Left(sameAs: SameAs)            => json"""{"sameAs": ${sameAs.toString}}"""
    case Right(derivedFrom: DerivedFrom) => json"""{"derivedFrom": ${derivedFrom.toString}}"""
  }

  def encoder(
      projectPath:      projects.Path
  )(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl): Encoder[ProjectDataset] =
    Encoder.instance[ProjectDataset] { case (id, originalId, title, name, sameAsOrDerived, images) =>
      json"""{
        "identifier": ${id.toString},
        "versions": {
          "initial": ${originalId.toString}
        },
        "title":  ${title.toString},
        "name":   ${name.toString},
        "images": ${images -> projectPath}
      }"""
        .deepMerge(sameAsOrDerived.asJson)
        .deepMerge(
          _links(
            Rel("details")         -> knowledgegraph.datasets.details.Endpoint.href(renkuApiUrl, id),
            Rel("initial-version") -> knowledgegraph.datasets.details.Endpoint.href(renkuApiUrl, originalId.value),
            Rel("tags") -> knowledgegraph.projects.datasets.tags.Endpoint.href(renkuApiUrl, projectPath, name)
          )
        )
    }
}
