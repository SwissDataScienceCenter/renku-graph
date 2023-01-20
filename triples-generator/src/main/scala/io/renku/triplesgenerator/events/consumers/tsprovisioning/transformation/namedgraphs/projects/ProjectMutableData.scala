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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.projects

import cats.data.{NonEmptyList => Nel}
import io.renku.graph.model.images.ImageResourceId
import io.renku.graph.model.versions.CliVersion
import io.renku.graph.model.{persons, projects}

private[projects] final case class ProjectMutableData(
    name:             projects.Name,
    dateCreated:      Nel[projects.DateCreated],
    maybeParentId:    Option[projects.ResourceId],
    visibility:       projects.Visibility,
    maybeDescription: Option[projects.Description],
    keywords:         Set[projects.Keyword],
    maybeAgent:       Option[CliVersion],
    maybeCreatorId:   Option[persons.ResourceId],
    images:           List[ImageResourceId]
) {

  lazy val earliestDateCreated: projects.DateCreated =
    dateCreated.toList.min

  def selectEarliestDateCreated: ProjectMutableData =
    copy(dateCreated = Nel.one(earliestDateCreated))
}
