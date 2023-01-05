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
import io.renku.graph.model.datasets.{Date, Description, Keyword, Name, ResourceId, TopmostSameAs}
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{persons, projects}

private final case class SearchInfo(topmostSameAs:    TopmostSameAs,
                                    name:             Name,
                                    visibility:       Visibility,
                                    date:             Date,
                                    creators:         NonEmptyList[PersonInfo],
                                    keywords:         List[Keyword],
                                    maybeDescription: Option[Description],
                                    images:           List[Image],
                                    links:            NonEmptyList[Link]
)

private final case class Link(resourceId: links.ResourceId, dataset: ResourceId, project: projects.ResourceId)
private object Link {
  def apply(topmostSameAs: TopmostSameAs,
            datasetId:     ResourceId,
            projectId:     projects.ResourceId,
            projectPath:   projects.Path
  ): Link = Link(links.ResourceId.from(topmostSameAs, projectPath), datasetId, projectId)
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

    def from(topmostSameAs: TopmostSameAs, projectPath: projects.Path): ResourceId = ResourceId(
      (topmostSameAs / projectPath).value
    )
  }
}

private final case class PersonInfo(resourceId: persons.ResourceId, name: persons.Name)

private object PersonInfo {
  lazy val toPersonInfo: Person => PersonInfo = p => PersonInfo(p.resourceId, p.name)
}
