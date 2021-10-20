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

package io.renku.commiteventservice.events.categories.common

import cats.MonadThrow
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.circe.literal._
import io.renku.commiteventservice.events.categories.commitsync.categoryName
import io.renku.commiteventservice.events.categories.common.UpdateResult._
import io.renku.events.EventRequestContent
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.events.EventStatus.AwaitingDeletion
import io.renku.tinytypes.json.TinyTypeEncoders
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[categories] trait CommitEventsRemover[Interpretation[_]] {
  def removeDeletedEvent(project: Project, commitId: CommitId): Interpretation[UpdateResult]
}

private class CommitEventsRemoverImpl[Interpretation[_]: MonadThrow](
    eventSender: EventSender[Interpretation]
) extends CommitEventsRemover[Interpretation]
    with TinyTypeEncoders {

  override def removeDeletedEvent(project: Project, commitId: CommitId): Interpretation[UpdateResult] =
    eventSender
      .sendEvent(
        EventRequestContent.NoPayload(json"""{
          "categoryName": "EVENTS_STATUS_CHANGE",
          "id":           $commitId,
          "project": {
            "id":   ${project.id},
            "path": ${project.path}
          },
          "newStatus": $AwaitingDeletion
        }"""),
        errorMessage = s"$categoryName: Marking event as $AwaitingDeletion failed"
      )
      .map(_ => Deleted: UpdateResult)
      .recoverWith { case NonFatal(e) =>
        Failed(s"$categoryName - Commit Remover failed to send commit deletion status", e)
          .pure[Interpretation]
          .widen[UpdateResult]
      }
}

private[categories] object CommitEventsRemover {
  def apply[Interpretation[_]: Async: Temporal: Logger]: Interpretation[CommitEventsRemover[Interpretation]] = for {
    sender <- EventSender[Interpretation]
  } yield new CommitEventsRemoverImpl[Interpretation](sender)
}
