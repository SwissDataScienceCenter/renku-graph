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

package io.renku.knowledgegraph.datasets

import io.renku.graph.model.datasets._
import io.renku.graph.model.persons.{Affiliation, Email, Name => UserName}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Path

object model {

  sealed trait Dataset extends Product with Serializable {
    val resourceId:       ResourceId
    val id:               Identifier
    val title:            Title
    val name:             Name
    val maybeDescription: Option[Description]
    val creators:         List[DatasetCreator]
    val date:             Date
    val parts:            List[DatasetPart]
    val project:          DatasetProject
    val usedIn:           List[DatasetProject]
    val keywords:         List[Keyword]
    val versions:         DatasetVersions
    val images:           List[ImageUri]

  }

  final case class NonModifiedDataset(resourceId:       ResourceId,
                                      id:               Identifier,
                                      title:            Title,
                                      name:             Name,
                                      sameAs:           SameAs,
                                      versions:         DatasetVersions,
                                      maybeDescription: Option[Description],
                                      creators:         List[DatasetCreator],
                                      date:             Date,
                                      parts:            List[DatasetPart],
                                      project:          DatasetProject,
                                      usedIn:           List[DatasetProject],
                                      keywords:         List[Keyword],
                                      images:           List[ImageUri]
  ) extends Dataset

  final case class ModifiedDataset(resourceId:       ResourceId,
                                   id:               Identifier,
                                   title:            Title,
                                   name:             Name,
                                   derivedFrom:      DerivedFrom,
                                   versions:         DatasetVersions,
                                   maybeDescription: Option[Description],
                                   creators:         List[DatasetCreator],
                                   date:             DateCreated,
                                   parts:            List[DatasetPart],
                                   project:          DatasetProject,
                                   usedIn:           List[DatasetProject],
                                   keywords:         List[Keyword],
                                   images:           List[ImageUri]
  ) extends Dataset

  final case class DatasetCreator(maybeEmail: Option[Email], name: UserName, maybeAffiliation: Option[Affiliation])

  final case class DatasetPart(location: PartLocation)
  final case class DatasetVersions(initial: OriginalIdentifier)

  final case class DatasetProject(path: Path, name: projects.Name)
}
