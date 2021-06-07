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

package ch.datascience.commiteventservice.events.categories.commitsync
package eventgeneration

import cats.MonadThrow
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.CommitId
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[commitsync] trait CommitEventsRemover[Interpretation[_]] {
  def removeDeletedEvent(project: Project, commitId: CommitId): Interpretation[UpdateResult]
}

private class CommitEventsRemoverImpl[Interpretation[_]: MonadThrow](
    eventStatusPatcher: EventStatusPatcher[Interpretation]
) extends CommitEventsRemover[Interpretation] {
  override def removeDeletedEvent(project: Project, commitId: CommitId): Interpretation[UpdateResult] =
    eventStatusPatcher
      .sendDeletionStatus(project.id, commitId)
      .map(_ => Deleted: UpdateResult) recoverWith { case NonFatal(e) =>
      Failed(s"$categoryName - Commit Remover failed to send commit deletion status", e)
        .pure[Interpretation]
        .widen[UpdateResult]
    }

}

private[commitsync] object CommitEventsRemover {

  def apply(logger:     Logger[IO])(implicit
      concurrentEffect: ConcurrentEffect[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[CommitEventsRemover[IO]] = for {
    eventStatusPatcher <- EventStatusPatcher(logger)
  } yield new CommitEventsRemoverImpl[IO](eventStatusPatcher)
}
