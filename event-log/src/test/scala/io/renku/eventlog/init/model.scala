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

package io.renku.eventlog.init

import io.renku.eventlog.{EventDate, EventMessage}
import io.renku.events.consumers.Project
import io.renku.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventId, EventStatus}

private object model {
  case class Event(id:           EventId,
                   project:      Project,
                   date:         EventDate,
                   batchDate:    BatchDate,
                   body:         EventBody,
                   status:       EventStatus,
                   maybeMessage: Option[EventMessage]
  ) {
    lazy val compoundEventId: CompoundEventId = CompoundEventId(id, project.id)
  }
}
