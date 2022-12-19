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

package io.renku.eventlog.events.producers.minprojectinfo

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.projects

private case class MinProjectInfoEvent(projectId: projects.GitLabId, projectPath: projects.Path)

private object MinProjectInfoEvent {
  implicit val show: Show[MinProjectInfoEvent] = Show.show { case MinProjectInfoEvent(id, path) =>
    show"projectId = $id, projectPath = $path"
  }
}

private object EventEncoder {

  import io.circe.Json
  import io.circe.literal._

  def encodeEvent(event: MinProjectInfoEvent): Json = json"""{
    "categoryName": ${categoryName.value},
    "project": {
      "id":   ${event.projectId.value},
      "path": ${event.projectPath.value}
    }
  }"""
}
