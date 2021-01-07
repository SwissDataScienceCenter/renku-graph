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

package io.renku.eventlog.subscriptions.unprocessed

import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import io.circe.Encoder

private final case class UnprocessedEvent(id: CompoundEventId, body: EventBody) {
  override lazy val toString: String = id.toString
}

private object UnprocessedEventEncoder extends Encoder[UnprocessedEvent] {

  import io.circe.Json
  import io.circe.literal.JsonStringContext

  override def apply(unprocessedEvent: UnprocessedEvent): Json =
    json"""{
        "id":      ${unprocessedEvent.id.id.value},
        "project": {
          "id":    ${unprocessedEvent.id.projectId.value}
        },
        "body":    ${unprocessedEvent.body.value}
      }"""
}
