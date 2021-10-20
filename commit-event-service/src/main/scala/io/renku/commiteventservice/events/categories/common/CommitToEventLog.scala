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
import cats.effect._
import cats.syntax.all._
import io.renku.commiteventservice.events.categories.commitsync.categoryName
import io.renku.commiteventservice.events.categories.common.CommitEvent.{NewCommitEvent, SkippedCommitEvent}
import io.renku.commiteventservice.events.categories.common.UpdateResult._
import io.renku.events.consumers.Project
import io.renku.graph.model.events.BatchDate
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[categories] trait CommitToEventLog[Interpretation[_]] {
  def storeCommitInEventLog(project:     Project,
                            startCommit: CommitInfo,
                            batchDate:   BatchDate
  ): Interpretation[UpdateResult]
}

private[categories] class CommitToEventLogImpl[Interpretation[_]: MonadThrow](
    commitEventSender: CommitEventSender[Interpretation]
) extends CommitToEventLog[Interpretation] {

  import commitEventSender._

  def storeCommitInEventLog(project:     Project,
                            startCommit: CommitInfo,
                            batchDate:   BatchDate
  ): Interpretation[UpdateResult] = {
    val commitEvent = toCommitEvent(project, batchDate)(startCommit)
    send(commitEvent)
      .map(_ => Created)
      .widen[UpdateResult]
      .recover { case NonFatal(exception) => Failed(failureMessageFor(commitEvent), exception) }
  }

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

  private def failureMessageFor(startCommit: CommitEvent) =
    s"$categoryName: id = ${startCommit.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> storing in the event log failed"
}
private[categories] object CommitToEventLog {
  def apply[Interpretation[_]: Async: Temporal: Logger]: Interpretation[CommitToEventLog[Interpretation]] = for {
    eventSender <- CommitEventSender[Interpretation]
  } yield new CommitToEventLogImpl[Interpretation](eventSender)
}
