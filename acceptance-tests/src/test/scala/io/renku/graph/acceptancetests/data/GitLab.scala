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

package io.renku.graph.acceptancetests.data

import io.circe.Json
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonEmptyStrings, positiveInts}
import io.renku.graph.model.GraphModelGenerators.personEmails
import io.renku.graph.model.events.CommitId

object GitLab {

  def pushEvent(project: Project, commitId: CommitId): Json = json"""{
    "after":         ${commitId.value},
    "user_id":       ${positiveInts().generateOne.value}, 
    "user_username": ${nonEmptyStrings().generateOne},
    "user_email":    ${personEmails.generateOne.value},
    "project": {
      "id":                  ${project.id.value},
      "path_with_namespace": ${project.path.value}
    }
  }"""
}
