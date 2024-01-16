/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.zombieevents

import io.renku.graph.model.{events, projects}
import io.renku.graph.model.events.CompoundEventId
import io.renku.graph.model.events.EventStatus.ProcessingStatus

private final case class ZombieEvent(eventId: CompoundEventId, projectSlug: projects.Slug, status: ProcessingStatus)

private object ZombieEvent {

  import cats.Show
  import cats.syntax.all._
  import io.circe.Decoder
  import io.renku.tinytypes.json.TinyTypeDecoders._

  implicit lazy val eventDecoder: Decoder[ZombieEvent] = { cursor =>
    implicit val statusDecoder: Decoder[events.EventStatus.ProcessingStatus] =
      Decoder[events.EventStatus].emap {
        case s: ProcessingStatus => s.asRight
        case s => show"'$s' is not a ProcessingStatus".asLeft
      }

    for {
      id          <- cursor.downField("id").as[events.EventId]
      projectId   <- cursor.downField("project").downField("id").as[projects.GitLabId]
      projectSlug <- cursor.downField("project").downField("slug").as[projects.Slug]
      status      <- cursor.downField("status").as[events.EventStatus.ProcessingStatus]
    } yield ZombieEvent(CompoundEventId(id, projectId), projectSlug, status)
  }

  implicit lazy val show: Show[ZombieEvent] = Show.show { event =>
    show"${event.eventId}, projectSlug = ${event.projectSlug}, status = ${event.status}"
  }
}
