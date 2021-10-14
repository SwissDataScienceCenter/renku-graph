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

package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.common.UpdateResult.Failed
import ch.datascience.commiteventservice.events.categories.common.{CommitEventsRemover, SynchronizationSummary}
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.CommitId
import ch.datascience.http.client.AccessToken
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[eventgeneration] trait CommitEventDeleter[Interpretation[_]] {
  def deleteExtraneousCommits(project: Project, commitsToDelete: List[CommitId])(implicit
      maybeAccessToken:                Option[AccessToken]
  ): Interpretation[SynchronizationSummary]
}
private[eventgeneration] class CommitEventDeleterImpl[Interpretation[_]: MonadThrow](
    commitEventsRemover: CommitEventsRemover[Interpretation]
) extends CommitEventDeleter[Interpretation] {

  import commitEventsRemover._

  override def deleteExtraneousCommits(project: Project, commitsToDelete: List[CommitId])(implicit
      maybeAccessToken:                         Option[AccessToken]
  ): Interpretation[SynchronizationSummary] = commitsToDelete.foldLeftM(SynchronizationSummary()) { (summary, commit) =>
    removeDeletedEvent(project, commit)
      .map(summary.incrementCount)
      .recoverWith { case NonFatal(error) =>
        summary.incrementCount(Failed(s"Failed to delete commit $commit", error)).pure[Interpretation]
      }
  }
}
private[eventgeneration] object CommitEventDeleter {
  def apply()(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      logger:           Logger[IO]
  ): IO[CommitEventDeleter[IO]] = for {
    commitEventsRemover <- CommitEventsRemover()
  } yield new CommitEventDeleterImpl[IO](commitEventsRemover)
}
