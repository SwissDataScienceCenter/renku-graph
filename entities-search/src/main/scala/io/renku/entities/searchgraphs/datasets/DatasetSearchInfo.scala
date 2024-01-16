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

import cats.Show
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.graph.model.images.Image
import io.renku.graph.model.{datasets, projects}

private final case class DatasetSearchInfo(topmostSameAs:      datasets.TopmostSameAs,
                                           name:               datasets.Name,
                                           slug:               datasets.Slug,
                                           createdOrPublished: datasets.CreatedOrPublished,
                                           maybeDateModified:  Option[datasets.DateModified],
                                           creators:           NonEmptyList[Creator],
                                           keywords:           List[datasets.Keyword],
                                           maybeDescription:   Option[datasets.Description],
                                           images:             List[Image],
                                           links:              NonEmptyList[Link]
) {
  val visibility: projects.Visibility = links.map(_.visibility).maximum
}

private object DatasetSearchInfo {

  implicit val show: Show[DatasetSearchInfo] = Show.show {
    case info @ DatasetSearchInfo(topSameAs,
                                  name,
                                  slug,
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
        show"slug = $slug".some,
        show"visibility = ${info.visibility}".some,
        createdOrPublished match {
          case d: datasets.DateCreated   => show"dateCreated = $d".some
          case d: datasets.DatePublished => show"datePublished = $d".some
        },
        maybeDateModified.map(d => show"dateModified = $d"),
        show"creators = [${creators.mkString_("; ")}]".some,
        keywords match {
          case Nil => None
          case k   => show"keywords = [${k.mkString("; ")}]".some
        },
        maybeDescription.map(d => show"description = $d"),
        images match {
          case Nil => None
          case i   => show"images = [${i.sortBy(_.position).map(_.uri).mkString_("; ")}]".some
        },
        show"links = [${links.mkString_("; ")}]".some
      ).flatten.mkString(", ")
  }
}
