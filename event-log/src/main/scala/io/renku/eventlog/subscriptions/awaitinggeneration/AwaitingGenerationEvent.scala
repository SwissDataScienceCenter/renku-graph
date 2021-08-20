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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.Show
import cats.syntax.all._
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import io.renku.eventlog.subscriptions.EventEncoder

private final case class AwaitingGenerationEvent(id: CompoundEventId, projectPath: projects.Path, body: EventBody) {
  override lazy val toString: String = s"$AwaitingGenerationEvent $id, projectPath = $projectPath"
}

private object AwaitingGenerationEvent {
  implicit lazy val show: Show[AwaitingGenerationEvent] =
    Show.show(event => show"AwaitingGenerationEvent ${event.id}, ${event.projectPath}")
}

private object AwaitingGenerationEventEncoder extends EventEncoder[AwaitingGenerationEvent] {

  import io.circe.Json
  import io.circe.literal.JsonStringContext

  override def encodeEvent(event: AwaitingGenerationEvent): Json = json"""{
    "categoryName": ${SubscriptionCategory.name.value},
    "id":           ${event.id.id.value},
    "project": {
      "id":         ${event.id.projectId.value}
    }
  }"""

  override def encodePayload(event: AwaitingGenerationEvent): Option[String] =
    event.body.value.some
}
