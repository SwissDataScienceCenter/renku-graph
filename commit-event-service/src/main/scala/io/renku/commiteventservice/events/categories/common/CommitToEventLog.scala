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

package io.renku.commiteventservice.events.categories.common

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.renku.commiteventservice.events.categories.commitsync.categoryName
import io.renku.commiteventservice.events.categories.common.CommitEvent.{NewCommitEvent, SkippedCommitEvent}
import io.renku.commiteventservice.events.categories.common.UpdateResult.{Created, Failed}
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.model.events.{BatchDate, CommitId, EventBody}
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[categories] trait CommitToEventLog[F[_]] {
  def storeCommitInEventLog(project: Project, startCommit: CommitInfo, batchDate: BatchDate): F[UpdateResult]
}

private[categories] class CommitToEventLogImpl[F[_]: MonadThrow](
    eventSender:     EventSender[F],
    eventSerializer: CommitEventSerializer
) extends CommitToEventLog[F] {

  import eventSender._
  import eventSerializer._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._

  def storeCommitInEventLog(project: Project, startCommit: CommitInfo, batchDate: BatchDate): F[UpdateResult] = {
    for {
      event     <- toCommitEvent(project, batchDate)(startCommit).pure[F]
      eventBody <- MonadThrow[F].fromEither(EventBody from serialiseToJsonString(event))
      _ <- sendEvent(
             EventRequestContent.NoPayload((event -> eventBody).asJson),
             EventSender.EventContext(CategoryName("CREATION"), failureMessage(event.id, event.project))
           )
    } yield Created: UpdateResult
  } recoverWith failedResult(startCommit, project)

  private def toCommitEvent(project: Project, batchDate: BatchDate)(commitInfo: CommitInfo) =
    commitInfo.message.value match {
      case message if message contains "renku migrate" =>
        SkippedCommitEvent(
          id = commitInfo.id,
          message = commitInfo.message,
          committedDate = commitInfo.committedDate,
          author = commitInfo.author,
          committer = commitInfo.committer,
          parents = commitInfo.parents,
          project = project,
          batchDate = batchDate
        )
      case _ =>
        NewCommitEvent(
          id = commitInfo.id,
          message = commitInfo.message,
          committedDate = commitInfo.committedDate,
          author = commitInfo.author,
          committer = commitInfo.committer,
          parents = commitInfo.parents,
          project = project,
          batchDate = batchDate
        )
    }

  private implicit lazy val entityEncoder: Encoder[(CommitEvent, EventBody)] =
    Encoder.instance[(CommitEvent, EventBody)] {
      case (event: NewCommitEvent, body) => json"""{
        "categoryName": "CREATION", 
        "id":        ${event.id.value},
        "project": {
          "id":      ${event.project.id.value},
          "path":    ${event.project.path.value}
        },
        "date":      ${event.committedDate.value},
        "batchDate": ${event.batchDate.value},
        "body":      ${body.value},
        "status":    ${event.status.value}
      }"""
      case (event: SkippedCommitEvent, body) => json"""{
        "categoryName": "CREATION",
        "id":        ${event.id.value},
        "project": {
          "id":      ${event.project.id.value},
          "path":    ${event.project.path.value}
        },
        "date":      ${event.committedDate.value},
        "batchDate": ${event.batchDate.value},
        "body":      ${body.value},
        "status":    ${event.status.value},
        "message":   ${event.message.value}
      }"""
    }

  private def failureMessage(eventId: CommitId, project: Project) =
    show"$categoryName: creating event id = $eventId, $project in event-log failed"

  private def failedResult(commitInfo: CommitInfo, project: Project): PartialFunction[Throwable, F[UpdateResult]] = {
    case NonFatal(exception) =>
      Failed(failureMessage(commitInfo.id, project), exception).pure[F].widen[UpdateResult]
  }
}

private[categories] object CommitToEventLog {
  def apply[F[_]: Async: Logger: MetricsRegistry]: F[CommitToEventLog[F]] = for {
    eventSender <- EventSender[F]
  } yield new CommitToEventLogImpl[F](eventSender, CommitEventSerializer)
}
