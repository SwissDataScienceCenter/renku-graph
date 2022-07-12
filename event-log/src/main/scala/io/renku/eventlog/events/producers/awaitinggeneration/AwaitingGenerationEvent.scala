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

package io.renku.eventlog.events.producers.awaitinggeneration

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.events.{CompoundEventId, EventBody}
import io.renku.graph.model.projects

private final case class AwaitingGenerationEvent(id: CompoundEventId, projectPath: projects.Path, body: EventBody)

private object AwaitingGenerationEvent {
  implicit lazy val show: Show[AwaitingGenerationEvent] = Show.show { event =>
    show"${event.id}, projectPath = ${event.projectPath}"
  }
}

private object AwaitingGenerationEventEncoder {

  import io.circe.Json
  import io.circe.literal.JsonStringContext

  def encodeEvent(event: AwaitingGenerationEvent): Json = json"""{
    "categoryName": ${SubscriptionCategory.categoryName.value},
    "id":           ${event.id.id.value},
    "project": {
      "id":         ${event.id.projectId.value}
    }
  }"""

  def encodePayload(event: AwaitingGenerationEvent): String = event.body.value
}