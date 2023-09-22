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

package io.renku.graph.model.gitlab

import io.renku.graph.model.persons.{Email, GitLabId, Name, Username}
import io.renku.graph.model.projects.Role

final case class GitLabUser(
    name:     Name,
    username: Username,
    gitLabId: GitLabId,
    email:    Option[Email]
) {
  def withEmail(email:      Email): GitLabUser   = copy(email = Some(email))
  def toMember(accessLevel: Int):   GitLabMember = GitLabMember(name, username, gitLabId, email, accessLevel)
  def toMember(role:        Role):  GitLabMember = toMember(Role.toGitLabAccessLevel(role))
}
