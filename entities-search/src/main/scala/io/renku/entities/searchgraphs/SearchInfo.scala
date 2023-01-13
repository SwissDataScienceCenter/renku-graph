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

import SearchInfo.DateModified
import cats.Show
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.graph.model.datasets.{Date, Description, Keyword, Name, ResourceId, TopmostSameAs}
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.graph.model.{datasets, persons, projects}
import io.renku.tinytypes.constraints.InstantNotInTheFuture
import io.renku.tinytypes.{InstantTinyType, TinyTypeFactory}

import java.time.Instant

private final case class SearchInfo(topmostSameAs:     TopmostSameAs,
                                    name:              Name,
                                    dateOriginal:      Date,
                                    maybeDateModified: Option[DateModified],
                                    creators:          NonEmptyList[PersonInfo],
                                    keywords:          List[Keyword],
                                    maybeDescription:  Option[Description],
                                    images:            List[Image],
                                    links:             NonEmptyList[Link]
) {
  lazy val visibility: Visibility = links.map(_.visibility).toList.max
}

private object SearchInfo {

  final class DateModified private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object DateModified
      extends TinyTypeFactory[DateModified](new DateModified(_))
      with InstantNotInTheFuture[DateModified]
      with TinyTypeJsonLDOps[DateModified] {

    def apply(date: datasets.DateCreated): DateModified = DateModified(date.instant)
  }

  implicit val show: Show[SearchInfo] = Show.show {
    case info @ SearchInfo(topSameAs,
                           name,
                           dateOriginal,
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
        show"visibility = ${info.visibility}".some,
        dateOriginal match {
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
  val datasetId:  ResourceId
  val projectId:  projects.ResourceId
  val visibility: projects.Visibility
}
private object Link {
  def apply(topmostSameAs: TopmostSameAs,
            datasetId:     ResourceId,
            projectId:     projects.ResourceId,
            projectPath:   projects.Path,
            visibility:    projects.Visibility
  ): Link =
    if (topmostSameAs.value == datasetId.value)
      OriginalDataset(links.ResourceId.from(topmostSameAs, projectPath), datasetId, projectId, visibility)
    else
      ImportedDataset(links.ResourceId.from(topmostSameAs, projectPath), datasetId, projectId, visibility)

  def apply(linkId:     links.ResourceId,
            datasetId:  ResourceId,
            projectId:  projects.ResourceId,
            visibility: projects.Visibility
  ): Link =
    if (linkId.value startsWith datasetId.value)
      OriginalDataset(linkId, datasetId, projectId, visibility)
    else
      ImportedDataset(linkId, datasetId, projectId, visibility)

  final case class OriginalDataset(resourceId: links.ResourceId,
                                   datasetId:  ResourceId,
                                   projectId:  projects.ResourceId,
                                   visibility: projects.Visibility
  ) extends Link
  final case class ImportedDataset(resourceId: links.ResourceId,
                                   datasetId:  ResourceId,
                                   projectId:  projects.ResourceId,
                                   visibility: projects.Visibility
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

    def from(topmostSameAs: TopmostSameAs, projectPath: projects.Path): ResourceId = ResourceId(
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
