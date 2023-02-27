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
package projecteventstonew

import cats.Show
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.circe.literal._
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent
import io.renku.events.consumers.Project
import io.renku.events.EventRequestContent
import io.renku.graph.model.{events, projects}
import io.renku.graph.model.events.EventStatus._
import io.renku.tinytypes.json.TinyTypeDecoders._

private[statuschange] final case class ProjectEventsToNew(project: Project) extends StatusChangeEvent {
  override val silent: Boolean = false
}

private[statuschange] object ProjectEventsToNew {

  val decoder: EventRequestContent => Either[DecodingFailure, ProjectEventsToNew] = { request =>
    for {
      projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
             case New    => Right(())
             case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield ProjectEventsToNew(Project(projectId, projectPath))
  }

  implicit lazy val encoder: Encoder[ProjectEventsToNew] = Encoder.instance {
    case ProjectEventsToNew(Project(id, path)) => json"""{
      "project": {
        "id":   ${id.value},
        "path": ${path.value}
      }
    }"""
  }
  implicit lazy val eventDecoder: Decoder[ProjectEventsToNew] = cursor =>
    for {
      id   <- cursor.downField("project").downField("id").as[projects.GitLabId]
      path <- cursor.downField("project").downField("path").as[projects.Path]
    } yield ProjectEventsToNew(Project(id, path))

  implicit lazy val eventType: StatusChangeEventsQueue.EventType[ProjectEventsToNew] =
    StatusChangeEventsQueue.EventType("PROJECT_EVENTS_TO_NEW")

  implicit lazy val show: Show[ProjectEventsToNew] = Show.show { case ProjectEventsToNew(project) =>
    show"$project, status = $New"
  }
}
