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
package totriplesgenerated

import cats.Show
import io.circe.DecodingFailure
import io.renku.events.EventRequestContent
import io.renku.graph.model.{events, projects}
import io.renku.graph.model.events.{CompoundEventId, EventProcessingTime, ZippedEventPayload}
import io.renku.graph.model.events.EventStatus._
import io.renku.tinytypes.json.TinyTypeDecoders._

private final case class ToTriplesGenerated(eventId:        CompoundEventId,
                                            projectPath:    projects.Path,
                                            processingTime: EventProcessingTime,
                                            payload:        ZippedEventPayload
) extends StatusChangeEvent {
  override val silent: Boolean = false
}

private object ToTriplesGenerated {

  val decoder: EventRequestContent => Either[DecodingFailure, ToTriplesGenerated] = {
    case EventRequestContent.WithPayload(event, payload: ZippedEventPayload) =>
      for {
        id             <- event.hcursor.downField("id").as[events.EventId]
        projectId      <- event.hcursor.downField("project").downField("id").as[projects.GitLabId]
        projectPath    <- event.hcursor.downField("project").downField("path").as[projects.Path]
        processingTime <- event.hcursor.downField("processingTime").as[EventProcessingTime]
        _ <- event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
               case TriplesGenerated => Right(())
               case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ToTriplesGenerated(CompoundEventId(id, projectId), projectPath, processingTime, payload)
    case _ => Left(DecodingFailure("Missing event payload", Nil))
  }

  implicit lazy val show: Show[ToTriplesGenerated] = Show.show { case ToTriplesGenerated(eventId, projectPath, _, _) =>
    s"$eventId, projectPath = $projectPath, status = $TriplesGenerated - update"
  }
}
