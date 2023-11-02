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

package io.renku.entities.searchgraphs.datasets

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.{datasets, projects}

private sealed trait Link {
  val resourceId:  links.ResourceId
  val datasetId:   datasets.ResourceId
  val projectId:   projects.ResourceId
  val projectSlug: projects.Slug
  val visibility:  projects.Visibility
}

private object Link {

  def apply(linkId:      links.ResourceId,
            datasetId:   datasets.ResourceId,
            projectId:   projects.ResourceId,
            projectSlug: projects.Slug,
            visibility:  projects.Visibility
  ): Link =
    if (linkId.value startsWith datasetId.value)
      OriginalDataset(linkId, datasetId, projectId, projectSlug, visibility)
    else
      ImportedDataset(linkId, datasetId, projectId, projectSlug, visibility)

  def from(topmostSameAs: datasets.TopmostSameAs,
           datasetId:     datasets.ResourceId,
           projectId:     projects.ResourceId,
           projectSlug:   projects.Slug,
           visibility:    projects.Visibility
  ): Link =
    if (topmostSameAs.value == datasetId.value)
      OriginalDataset(links.ResourceId.from(topmostSameAs, projectSlug), datasetId, projectId, projectSlug, visibility)
    else
      ImportedDataset(links.ResourceId.from(topmostSameAs, projectSlug), datasetId, projectId, projectSlug, visibility)

  final case class OriginalDataset(resourceId:  links.ResourceId,
                                   datasetId:   datasets.ResourceId,
                                   projectId:   projects.ResourceId,
                                   projectSlug: projects.Slug,
                                   visibility:  projects.Visibility
  ) extends Link
  final case class ImportedDataset(resourceId:  links.ResourceId,
                                   datasetId:   datasets.ResourceId,
                                   projectId:   projects.ResourceId,
                                   projectSlug: projects.Slug,
                                   visibility:  projects.Visibility
  ) extends Link

  implicit lazy val show: Show[Link] = Show.show { link =>
    show"id = ${link.resourceId}, projectId = ${link.projectId}, datasetId = ${link.datasetId}, visibility = ${link.visibility}"
  }
}

private object links {
  import io.renku.graph.model.views.EntityIdJsonLDOps
  import io.renku.tinytypes.constraints.{Url => UrlConstraint}
  import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with UrlConstraint[ResourceId]
      with EntityIdJsonLDOps[ResourceId] {

    def from(topmostSameAs: datasets.TopmostSameAs, projectSlug: projects.Slug): ResourceId = ResourceId(
      (topmostSameAs / projectSlug).value
    )
  }
}
