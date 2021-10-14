/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.membersync

import cats.Show
import cats.implicits.showInterpolator
import ch.datascience.graph.model.projects

private final case class MemberSyncEvent(projectPath: projects.Path)

private object MemberSyncEvent {
  implicit lazy val show: Show[MemberSyncEvent] =
    Show.show(event => show"MemberSyncEvent projectPath = ${event.projectPath}")
}

private object MemberSyncEventEncoder {

  import io.circe.Json
  import io.circe.literal.JsonStringContext

  def encodeEvent(event: MemberSyncEvent): Json =
    json"""{
    "categoryName": ${categoryName.value},
    "project": {
      "path":       ${event.projectPath.value}
    }
  }"""
}
