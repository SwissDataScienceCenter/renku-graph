/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange
package tofailure

import cats.Show
import cats.syntax.all._
import io.circe.DecodingFailure
import ToFailure.AllowedCombination
import io.renku.events.EventRequestContent
import io.renku.graph.model.{events, projects}
import io.renku.graph.model.events.{CompoundEventId, EventMessage}
import io.renku.graph.model.events.EventStatus._
import io.renku.tinytypes.json.TinyTypeDecoders._

import java.time.Duration

private[statuschange] final case class ToFailure[+C <: ProcessingStatus, +N <: FailureStatus](
    eventId:             CompoundEventId,
    projectPath:         projects.Path,
    message:             EventMessage,
    currentStatus:       C,
    newStatus:           N,
    maybeExecutionDelay: Option[Duration]
)(implicit ev: AllowedCombination[C, N])
    extends StatusChangeEvent {
  override val silent: Boolean = false
}

private[statuschange] object ToFailure {

  sealed trait AllowedCombination[C <: ProcessingStatus, N <: FailureStatus]

  implicit object GenerationToNonRecoverableFailure
      extends AllowedCombination[GeneratingTriples, GenerationNonRecoverableFailure]

  implicit object GenerationToRecoverableFailure
      extends AllowedCombination[GeneratingTriples, GenerationRecoverableFailure]

  implicit object TransformationToNonRecoverableFailure
      extends AllowedCombination[TransformingTriples, TransformationNonRecoverableFailure]

  implicit object TransformationToRecoverableFailure
      extends AllowedCombination[TransformingTriples, TransformationRecoverableFailure]

  val decoder: EventRequestContent => Either[DecodingFailure, ToFailure[ProcessingStatus, FailureStatus]] = { request =>
    for {
      id          <- request.event.hcursor.downField("id").as[events.EventId]
      projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      message     <- request.event.hcursor.downField("message").as[EventMessage]
      eventId = CompoundEventId(id, projectId)
      executionDelay <- request.event.hcursor.downField("executionDelay").as[Option[Duration]]
      statusChangeEvent <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
                             case status: GenerationRecoverableFailure =>
                               ToFailure(eventId,
                                         projectPath,
                                         message,
                                         GeneratingTriples,
                                         status,
                                         executionDelay
                               ).asRight
                             case status: GenerationNonRecoverableFailure =>
                               ToFailure(eventId,
                                         projectPath,
                                         message,
                                         GeneratingTriples,
                                         status,
                                         maybeExecutionDelay = None
                               ).asRight
                             case status: TransformationRecoverableFailure =>
                               ToFailure(eventId,
                                         projectPath,
                                         message,
                                         TransformingTriples,
                                         status,
                                         executionDelay
                               ).asRight
                             case status: TransformationNonRecoverableFailure =>
                               ToFailure(eventId,
                                         projectPath,
                                         message,
                                         TransformingTriples,
                                         status,
                                         maybeExecutionDelay = None
                               ).asRight
                             case status =>
                               DecodingFailure(s"Unrecognized event status $status", Nil)
                                 .asLeft[ToFailure[ProcessingStatus, FailureStatus]]
                           }
    } yield statusChangeEvent
  }

  implicit lazy val show: Show[ToFailure[ProcessingStatus, FailureStatus]] = Show.show {
    case ToFailure(eventId, projectPath, _, _, newStatus, _) =>
      s"$eventId, projectPath = $projectPath, status = $newStatus - update"
  }
}
