/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.circe.Decoder
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.EventLogDB
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers._
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.CategoryName
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class EventHandler[Interpretation[_]: MonadThrow: ContextShift: Concurrent](
    override val categoryName: CategoryName,
    commitSyncForcer:          CommitSyncForcer[Interpretation],
    logger:                    Logger[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](ConcurrentProcessesLimiter.withoutLimit) {

  import commitSyncForcer._
  import io.renku.graph.model.projects
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def createHandlingProcess(
      request: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    EventHandlingProcess[Interpretation](startForceCommitSync(request))

  private def startForceCommitSync(request: EventRequestContent) = for {
    event <-
      fromEither[Interpretation](
        request.event.as[(projects.Id, projects.Path)].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
      )
    result <- forceCommitSync(event._1, event._2).toRightT
                .map(_ => Accepted)
                .semiflatTap(logger.log(event))
                .leftSemiflatTap(logger.log(event))
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
  def apply(sessionResource:  SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name],
            logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    commitSyncForcer <- CommitSyncForcer(sessionResource, queriesExecTimes)
  } yield new EventHandler[IO](categoryName, commitSyncForcer, logger)
}
