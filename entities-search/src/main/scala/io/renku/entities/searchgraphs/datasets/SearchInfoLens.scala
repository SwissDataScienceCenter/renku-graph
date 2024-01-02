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

package io.renku.entities.searchgraphs.datasets

import cats.data.NonEmptyList
import io.renku.graph.model.{entities, projects}
import monocle.Lens

private object SearchInfoLens {

  val searchInfoLinks: Lens[DatasetSearchInfo, NonEmptyList[Link]] =
    Lens[DatasetSearchInfo, NonEmptyList[Link]](_.links) { links => info =>
      info.copy(links = links)
    }

  val modelSearchInfoLink: Lens[ModelDatasetSearchInfo, Link] =
    Lens[ModelDatasetSearchInfo, Link](_.link)(link => _.copy(link = link))

  val tsSearchInfoLinks: Lens[TSDatasetSearchInfo, List[Link]] =
    Lens[TSDatasetSearchInfo, List[Link]](_.links) { links => info =>
      info.copy(links = links)
    }

  val linkProjectId: Lens[Link, projects.ResourceId] =
    Lens[Link, projects.ResourceId](_.projectId) { projectId =>
      {
        case link: Link.OriginalDataset => link.copy(projectId = projectId)
        case link: Link.ImportedDataset => link.copy(projectId = projectId)
      }
    }

  val linkProjectSlug: Lens[Link, projects.Slug] =
    Lens[Link, projects.Slug](_.projectSlug) { projectSlug =>
      {
        case link: Link.OriginalDataset => link.copy(projectSlug = projectSlug)
        case link: Link.ImportedDataset => link.copy(projectSlug = projectSlug)
      }
    }

  val linkVisibility: Lens[Link, projects.Visibility] =
    Lens[Link, projects.Visibility](_.visibility) { visibility =>
      {
        case link: Link.OriginalDataset => link.copy(visibility = visibility)
        case link: Link.ImportedDataset => link.copy(visibility = visibility)
      }
    }

  def updateLinkProject(project: entities.Project): Link => Link =
    (linkProjectId replace project.resourceId) andThen
      (linkVisibility replace project.visibility) andThen
      (linkProjectSlug replace project.slug)
}
