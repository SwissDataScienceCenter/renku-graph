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
package toawaitingdeletion

import cats.Show
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.events.EventRequestContent
import io.renku.graph.model.{events, projects}
import io.renku.graph.model.events.{CompoundEventId, EventStatus}
import io.renku.graph.model.events.EventStatus._
import io.renku.tinytypes.json.TinyTypeDecoders._

private[statuschange] final case class ToAwaitingDeletion(eventId: CompoundEventId, projectPath: projects.Path)
    extends StatusChangeEvent {
  override val silent: Boolean = false

  def toRaw: RawStatusChangeEvent =
    RawStatusChangeEvent(
      Some(eventId.id),
      Some(RawStatusChangeEvent.Project(eventId.projectId.some, projectPath)),
      None,
      None,
      None,
      EventStatus.AwaitingDeletion
    )
}

private[statuschange] object ToAwaitingDeletion {
  def unapply(raw: RawStatusChangeEvent): Option[ToAwaitingDeletion] =
    raw match {
      case RawStatusChangeEvent(
            Some(id),
            Some(RawStatusChangeEvent.Project(Some(pid), path)),
            None,
            None,
            None,
            EventStatus.AwaitingDeletion
          ) =>
        ToAwaitingDeletion(CompoundEventId(id, pid), path).some
      case _ => None
    }

  val decoder: EventRequestContent => Either[DecodingFailure, ToAwaitingDeletion] = { request =>
    for {
      id          <- request.event.hcursor.downField("id").as[events.EventId]
      projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
             case AwaitingDeletion => Right(())
             case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield ToAwaitingDeletion(CompoundEventId(id, projectId), projectPath)
  }

  implicit lazy val show: Show[ToAwaitingDeletion] = Show.show { case ToAwaitingDeletion(eventId, projectPath) =>
    s"$eventId, projectPath = $projectPath, status = $AwaitingDeletion"
  }
}
