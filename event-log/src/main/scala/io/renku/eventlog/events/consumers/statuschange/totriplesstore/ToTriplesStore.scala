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
package totriplesstore

import cats.Show
import io.circe.DecodingFailure
import io.renku.events.EventRequestContent
import io.renku.graph.model.{events, projects}
import io.renku.graph.model.events.{CompoundEventId, EventProcessingTime}
import io.renku.graph.model.events.EventStatus._
import io.renku.tinytypes.json.TinyTypeDecoders._

private[statuschange] final case class ToTriplesStore(
    eventId:        CompoundEventId,
    projectPath:    projects.Path,
    processingTime: EventProcessingTime
) extends StatusChangeEvent {
  override val silent: Boolean = false
}

private[statuschange] object ToTriplesStore {

  val decoder: EventRequestContent => Either[DecodingFailure, ToTriplesStore] = { request =>
    for {
      id             <- request.event.hcursor.downField("id").as[events.EventId]
      projectId      <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
      projectPath    <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      processingTime <- request.event.hcursor.downField("processingTime").as[EventProcessingTime]
      _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
             case TriplesStore => Right(())
             case status       => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield ToTriplesStore(CompoundEventId(id, projectId), projectPath, processingTime)
  }

  implicit lazy val show: Show[ToTriplesStore] = Show.show { case ToTriplesStore(eventId, projectPath, _) =>
    s"$eventId, projectPath = $projectPath, status = $TriplesStore - update"
  }
}
