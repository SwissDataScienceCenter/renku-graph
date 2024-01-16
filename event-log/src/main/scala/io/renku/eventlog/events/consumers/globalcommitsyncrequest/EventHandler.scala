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

package io.renku.eventlog.events.consumers.globalcommitsyncrequest

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.consumers.EventDecodingTools._
import io.renku.events.consumers._
import io.renku.events.{CategoryName, consumers}
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    globalCommitSyncForcer:    GlobalCommitSyncForcer[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](ProcessExecutor.sequential) {

  protected override type Event = Project

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.getProject,
      process = startForceCommitSync
    )

  private def startForceCommitSync: Event => F[Unit] = { case project @ Project(projectId, projectSlug) =>
    Logger[F].info(show"$categoryName: $project accepted") >>
      globalCommitSyncForcer.moveGlobalCommitSync(projectId, projectSlug)
  }
}

private object EventHandler {
  def apply[F[_]: Async: SessionResource: Logger: QueriesExecutionTimes]: F[consumers.EventHandler[F]] =
    GlobalCommitSyncForcer[F]().map(new EventHandler[F](_))
}
