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

package io.renku.eventlog.statuschange

import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import io.renku.eventlog.EventMessage
import io.renku.eventlog.statuschange.commands.ChangeStatusCommand

sealed trait CommandFindingResult extends Product with Serializable

private object CommandFindingResult {
  case class CommandFound[Interpretation[_]](command: ChangeStatusCommand[Interpretation]) extends CommandFindingResult
  case object NotSupported extends CommandFindingResult
  case class PayloadMalformed(message: String) extends CommandFindingResult
}

sealed trait ChangeStatusRequest {
  def eventId: CompoundEventId
  def status:  EventStatus
}

private object ChangeStatusRequest {

  case class EventOnlyRequest(eventId:             CompoundEventId,
                              status:              EventStatus,
                              maybeProcessingTime: Option[EventProcessingTime],
                              maybeMessage:        Option[EventMessage]
  ) extends ChangeStatusRequest {
    def addPayload(payloadPartBody: String): EventAndPayloadRequest =
      EventAndPayloadRequest(eventId, status, maybeProcessingTime, payloadPartBody)
  }
  case class EventAndPayloadRequest(eventId:             CompoundEventId,
                                    status:              EventStatus,
                                    maybeProcessingTime: Option[EventProcessingTime],
                                    payloadPartBody:     String
  ) extends ChangeStatusRequest
}
