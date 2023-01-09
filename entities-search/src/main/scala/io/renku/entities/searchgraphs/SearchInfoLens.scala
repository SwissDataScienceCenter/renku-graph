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

package io.renku.entities.searchgraphs

import cats.data.NonEmptyList
import io.renku.graph.model.projects
import monocle.{Lens, Traversal}

private object SearchInfoLens {

  val searchInfoLinks: Lens[SearchInfo, NonEmptyList[Link]] = Lens[SearchInfo, NonEmptyList[Link]](_.links) {
    links => info => info.copy(links = links)
  }

  private val linksTraversal = Traversal.fromTraverse[NonEmptyList, Link]

  def replaceLinks(link: Link): NonEmptyList[Link] => NonEmptyList[Link] = _ => NonEmptyList.one(link)

  val linkProject: Lens[Link, projects.ResourceId] = Lens[Link, projects.ResourceId](_.projectId) { projectId =>
    {
      case link: Link.OriginalDataset => link.copy(projectId = projectId)
      case link: Link.ImportedDataset => link.copy(projectId = projectId)
    }
  }

  def findLink(projectId: projects.ResourceId): SearchInfo => Option[Link] =
    searchInfoLinks.composeTraversal(linksTraversal).find(_.projectId == projectId)
}
