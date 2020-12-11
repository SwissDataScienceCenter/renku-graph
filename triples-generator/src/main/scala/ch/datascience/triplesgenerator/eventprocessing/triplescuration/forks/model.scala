/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import ch.datascience.graph.model.projects.{DateCreated, Path}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.{Email, GitLabId}

private final case class GitLabProject(path:            Path,
                                       maybeParentPath: Option[Path],
                                       maybeCreator:    Option[GitLabCreator],
                                       dateCreated:     DateCreated
)

private final case class GitLabCreator(gitLabId: GitLabId, name: users.Name)

private final case class KGCreator(resourceId: users.ResourceId, maybeEmail: Option[Email], name: users.Name)
