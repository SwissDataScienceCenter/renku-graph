/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests.model

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{nonEmptyStrings, positiveInts}
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.{CommitId, Project, ProjectId}
import io.circe.Json
import io.circe.literal._

object GitLab {

  def pushEvent(projectId: ProjectId, commitId: CommitId): Json =
    pushEvent(projects.generateOne.copy(id = projectId), commitId)

  def pushEvent(project: Project, commitId: CommitId): Json = json"""
      {
        "after":         ${commitId.value},
        "user_id":       ${positiveInts().generateOne.value}, 
        "user_username": ${nonEmptyStrings().generateOne},
        "user_email":    ${emails.generateOne.value},
        "project": {
          "id":                  ${project.id.value},
          "path_with_namespace": ${project.path.value}
        }
      }"""
}
