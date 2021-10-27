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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.MonadThrow
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.renku.commiteventservice.events.categories.common.UpdateResult.Failed
import io.renku.commiteventservice.events.categories.common.{CommitEventsRemover, SynchronizationSummary}
import io.renku.events.consumers.Project
import io.renku.graph.model.events.CommitId
import io.renku.http.client.AccessToken
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[eventgeneration] trait CommitEventDeleter[F[_]] {
  def deleteExtraneousCommits(project: Project, commitsToDelete: List[CommitId])(implicit
      maybeAccessToken:                Option[AccessToken]
  ): F[SynchronizationSummary]
}
private[eventgeneration] class CommitEventDeleterImpl[F[_]: MonadThrow](
    commitEventsRemover: CommitEventsRemover[F]
) extends CommitEventDeleter[F] {

  import commitEventsRemover._

  override def deleteExtraneousCommits(project: Project, commitsToDelete: List[CommitId])(implicit
      maybeAccessToken:                         Option[AccessToken]
  ): F[SynchronizationSummary] = commitsToDelete.foldLeftM(SynchronizationSummary()) { (summary, commit) =>
    removeDeletedEvent(project, commit)
      .map(summary.incrementCount)
      .recoverWith { case NonFatal(error) =>
        summary.incrementCount(Failed(s"Failed to delete commit $commit", error)).pure[F]
      }
  }
}
private[eventgeneration] object CommitEventDeleter {
  def apply[F[_]: Async: Temporal: Logger]: F[CommitEventDeleter[F]] = for {
    commitEventsRemover <- CommitEventsRemover[F]
  } yield new CommitEventDeleterImpl[F](commitEventsRemover)
}
