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

package io.renku.eventlog.events.consumers.globalcommitsyncrequest

import cats.effect.{Concurrent, MonadCancelThrow}
import cats.syntax.all._
import io.circe.Decoder
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.{CategoryName, consumers}
import io.renku.events.consumers._
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    override val categoryName: CategoryName,
    globalCommitSyncForcer:    GlobalCommitSyncForcer[F]
) extends consumers.EventHandlerWithProcessLimiter[F](ProcessExecutor.sequential) {

  import globalCommitSyncForcer._
  import io.renku.graph.model.projects
  import io.renku.tinytypes.json.TinyTypeDecoders._

  protected override type Event = (projects.GitLabId, projects.Path)

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.as[(projects.GitLabId, projects.Path)],
      process = startForceCommitSync
    )

  private def startForceCommitSync: Event => F[Unit] = { case (projectId, projectPath) =>
    moveGlobalCommitSync(projectId, projectPath)
  }

  private implicit val eventDecoder: Decoder[Event] = { cursor =>
    (cursor.downField("project").downField("id").as[projects.GitLabId] ->
      cursor.downField("project").downField("path").as[projects.Path])
      .mapN(_ -> _)
  }
}

private object EventHandler {
  def apply[F[_]: Concurrent: SessionResource: Logger: QueriesExecutionTimes]: F[consumers.EventHandler[F]] =
    GlobalCommitSyncForcer[F]().map(new EventHandler[F](categoryName, _))
}
