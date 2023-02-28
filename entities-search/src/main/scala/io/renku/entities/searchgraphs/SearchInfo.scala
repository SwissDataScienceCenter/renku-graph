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

import cats.Show
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.Image
import io.renku.graph.model.{datasets, persons, projects}

private final case class SearchInfo(topmostSameAs:      datasets.TopmostSameAs,
                                    name:               datasets.Name,
                                    visibility:         projects.Visibility,
                                    createdOrPublished: datasets.CreatedOrPublished,
                                    maybeDateModified:  Option[datasets.DateModified],
                                    creators:           NonEmptyList[PersonInfo],
                                    keywords:           List[datasets.Keyword],
                                    maybeDescription:   Option[datasets.Description],
                                    images:             List[Image],
                                    links:              NonEmptyList[Link]
) {
  def findLink(projectId: projects.ResourceId): Option[Link] =
    links.find(_.projectId == projectId)
}

private object SearchInfo {

  implicit val show: Show[SearchInfo] = Show.show {
    case SearchInfo(topSameAs,
                    name,
                    visibility,
                    createdOrPublished,
                    maybeDateModified,
                    creators,
                    keywords,
                    maybeDescription,
                    images,
                    links
        ) =>
      List(
        show"topmostSameAs = $topSameAs".some,
        show"name = $name".some,
        show"visibility = $visibility".some,
        createdOrPublished match {
          case d: datasets.DateCreated   => show"dateCreated = $d".some
          case d: datasets.DatePublished => show"datePublished = $d".some
        },
        maybeDateModified.map(d => show"dateModified = $d"),
        show"creators = [${creators.map(_.show).intercalate("; ")}}]".some,
        keywords match {
          case Nil => None
          case k   => show"keywords = [${k.mkString(", ")}]".some
        },
        maybeDescription.map(d => show"description = $d"),
        images match {
          case Nil => None
          case i   => show"images = [${i.sortBy(_.position).map(i => show"${i.uri.value}").mkString(", ")}]".some
        },
        show"links = [${links.map(_.show).intercalate("; ")}}]".some
      ).flatten.mkString(", ")
  }
}

private sealed trait Link {
  val resourceId: links.ResourceId
  val datasetId:  datasets.ResourceId
  val projectId:  projects.ResourceId
}
private object Link {
  def apply(topmostSameAs: datasets.TopmostSameAs,
            datasetId:     datasets.ResourceId,
            projectId:     projects.ResourceId,
            projectPath:   projects.Path
  ): Link =
    if (topmostSameAs.value == datasetId.value)
      OriginalDataset(links.ResourceId.from(topmostSameAs, projectPath), datasetId, projectId)
    else
      ImportedDataset(links.ResourceId.from(topmostSameAs, projectPath), datasetId, projectId)

  def apply(linkId: links.ResourceId, datasetId: datasets.ResourceId, projectId: projects.ResourceId): Link =
    if (linkId.value startsWith datasetId.value)
      OriginalDataset(linkId, datasetId, projectId)
    else
      ImportedDataset(linkId, datasetId, projectId)

  final case class OriginalDataset(resourceId: links.ResourceId,
                                   datasetId:  datasets.ResourceId,
                                   projectId:  projects.ResourceId
  ) extends Link
  final case class ImportedDataset(resourceId: links.ResourceId,
                                   datasetId:  datasets.ResourceId,
                                   projectId:  projects.ResourceId
  ) extends Link

  implicit lazy val show: Show[Link] = Show.show { link =>
    show"id = ${link.resourceId}, projectId = ${link.projectId}, datasetId = ${link.datasetId}"
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

    def from(topmostSameAs: datasets.TopmostSameAs, projectPath: projects.Path): ResourceId = ResourceId(
      (topmostSameAs / projectPath).value
    )
  }
}

private final case class PersonInfo(resourceId: persons.ResourceId, name: persons.Name)

private object PersonInfo {
  lazy val toPersonInfo: Person => PersonInfo = p => PersonInfo(p.resourceId, p.name)
  implicit lazy val show: Show[PersonInfo] = Show.show { case PersonInfo(resourceId, name) =>
    show"id = $resourceId, name = $name"
  }
}
