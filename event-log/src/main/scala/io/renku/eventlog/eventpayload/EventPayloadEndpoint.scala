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

package io.renku.eventlog.eventpayload

import cats.effect.Concurrent
import cats.syntax.all._
import io.renku.data.Message
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.EventId
import io.renku.graph.model.projects.{Path => ProjectPath}
import org.http4s._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{`Content-Disposition`, `Content-Length`, `Content-Type`}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

trait EventPayloadEndpoint[F[_]] {

  def getEventPayload(eventId: EventId, projectPath: ProjectPath): F[Response[F]]
}

object EventPayloadEndpoint {

  def apply[F[_]: Concurrent: SessionResource: Logger: QueriesExecutionTimes]: EventPayloadEndpoint[F] =
    apply[F](EventPayloadFinder[F])

  def apply[F[_]: Concurrent: Logger](payloadFinder: EventPayloadFinder[F]): EventPayloadEndpoint[F] =
    new EventPayloadEndpoint[F] with Http4sDsl[F] {
      def getEventPayload(eventId: EventId, projectPath: ProjectPath): F[Response[F]] =
        payloadFinder.findEventPayload(eventId, projectPath).flatMap {
          case Some(data) =>
            for {
              resp <- Ok(data.data)
              name = s"${eventId.value}-${projectPath.value.replace('/', '_')}.gz"
              r = resp.withHeaders(
                    `Content-Length`(data.length),
                    `Content-Type`(MediaType.application.gzip),
                    `Content-Disposition`("attachment", Map(ci"filename" -> name))
                  )
            } yield r
          case None =>
            NotFound(Message.Info.unsafeApply(show"Event/Project $eventId/$projectPath not found"))
        }
    }
}
