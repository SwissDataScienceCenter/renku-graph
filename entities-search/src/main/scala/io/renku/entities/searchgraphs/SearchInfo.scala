/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

private sealed trait SearchInfo {
  val topmostSameAs:    TopmostSameAs
  val name:             Name
  val visibility:       Visibility
  val date:             Date
  val creators:         NonEmptyList[PersonInfo]
  val keywords:         List[Keyword]
  val maybeDescription: Option[Description]
  val images:           List[Image]
}

private object SearchInfo {

  final case class ProjectSearchInfo(topmostSameAs:    TopmostSameAs,
                                     name:             Name,
                                     visibility:       Visibility,
                                     date:             Date,
                                     creators:         NonEmptyList[PersonInfo],
                                     keywords:         List[Keyword],
                                     maybeDescription: Option[Description],
                                     images:           List[Image],
                                     link:             Link
  ) extends SearchInfo

  final case class StoreSearchInfo(topmostSameAs:    TopmostSameAs,
                                   name:             Name,
                                   visibility:       Visibility,
                                   date:             Date,
                                   creators:         NonEmptyList[PersonInfo],
                                   keywords:         List[Keyword],
                                   maybeDescription: Option[Description],
                                   images:           List[Image],
                                   links:            List[Link]
  ) extends SearchInfo
}

private final case class Link(dataset: ResourceId, project: projects.ResourceId)

private final case class PersonInfo(resourceId: persons.ResourceId, name: persons.Name)

private object PersonInfo {
  lazy val toPersonInfo: Person => PersonInfo = p => PersonInfo(p.resourceId, p.name)
}
