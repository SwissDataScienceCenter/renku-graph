/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.categories.commitsyncrequest

import cats.Show
import cats.data.EitherT.fromEither
import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.Decoder
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.EventLogDB
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers._
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.CategoryName
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: Concurrent: Logger](
    override val categoryName: CategoryName,
    commitSyncForcer:          CommitSyncForcer[F]
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  import commitSyncForcer._
  import io.renku.graph.model.projects
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess[F](startForceCommitSync(request))

  private def startForceCommitSync(request: EventRequestContent) = for {
    event <-
      fromEither[F](
        request.event.as[(projects.Id, projects.Path)].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
      )
    result <- forceCommitSync(event._1, event._2).toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(event))
                .leftSemiflatTap(Logger[F].log(event))
  } yield result

  private implicit lazy val eventInfoToString: Show[(projects.Id, projects.Path)] = Show.show {
    case (projectId, projectPath) => show"projectId = $projectId, projectPath = $projectPath"
  }

  private implicit val eventDecoder: Decoder[(projects.Id, projects.Path)] = { cursor =>
    for {
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
    } yield projectId -> projectPath
  }
}

private object EventHandler {
  def apply[F[_]: Concurrent: Logger](sessionResource: SessionResource[F, EventLogDB],
                                      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[EventHandler[F]] = for {
    commitSyncForcer <- CommitSyncForcer(sessionResource, queriesExecTimes)
  } yield new EventHandler[F](categoryName, commitSyncForcer)
}
