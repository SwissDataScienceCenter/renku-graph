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

package io.renku.eventlog.events.categories.creation

import io.renku.eventlog.{CompoundId, EventDate, EventMessage}
import io.renku.events.consumers.Project
import io.renku.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventId, EventStatus}

private sealed trait Event extends CompoundId {
  def id:        EventId
  def project:   Project
  def date:      EventDate
  def batchDate: BatchDate
  def body:      EventBody
  def status:    EventStatus

  def withBatchDate(batchDate: BatchDate): Event
  lazy val compoundEventId: CompoundEventId = CompoundEventId(id, project.id)
}

private object Event {

  final case class NewEvent(
      id:        EventId,
      project:   Project,
      date:      EventDate,
      batchDate: BatchDate,
      body:      EventBody,
      status:    EventStatus = EventStatus.New
  ) extends Event {

    override def withBatchDate(batchDate: BatchDate): Event = this.copy(batchDate = batchDate)

  }

  final case class SkippedEvent(
      id:        EventId,
      project:   Project,
      date:      EventDate,
      batchDate: BatchDate,
      body:      EventBody,
      message:   EventMessage
  ) extends Event {
    val status: EventStatus = EventStatus.Skipped

    override def withBatchDate(batchDate: BatchDate): Event = this.copy(batchDate = batchDate)
  }
}
