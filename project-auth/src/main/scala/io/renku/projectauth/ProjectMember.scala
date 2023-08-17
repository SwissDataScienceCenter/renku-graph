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

package io.renku.projectauth

import io.renku.graph.model.persons.GitLabId

final case class ProjectMember(
    gitLabId: GitLabId,
    role:     Role
) {

  private[projectauth] def encoded: String =
    s"${gitLabId.value}:${role.asString}"
}

object ProjectMember {
  private[projectauth] def fromEncoded(str: String): Either[String, ProjectMember] =
    str.split(':').toList match {
      case idStr :: roleStr :: Nil =>
        for {
          id   <- idStr.toIntOption.map(GitLabId.apply).toRight(s"Invalid person GitLabId: $idStr")
          role <- Role.fromString(roleStr)
        } yield ProjectMember(id, role)

      case _ =>
        Left(s"Invalid encoded project member: $str")
    }

  def fromGitLabData(gitLabId: GitLabId, accessLevel: Int): ProjectMember =
    ProjectMember(gitLabId, Role.fromGitLabAccessLevel(accessLevel))
}
