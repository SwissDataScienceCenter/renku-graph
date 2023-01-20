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

package io.renku.commiteventservice.events.consumers.globalcommitsync.eventgeneration

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.commiteventservice.events.consumers.common._
import io.renku.events.consumers.Project
import io.renku.graph.model.events.{BatchDate, CommitId}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private[eventgeneration] trait MissingCommitEventCreator[F[_]] {
  def createCommits(project: Project, commitsToCreate: List[CommitId])(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[SynchronizationSummary]
}

private[eventgeneration] class MissingCommitEventCreatorImpl[F[_]: MonadThrow](
    commitInfoFinder: CommitInfoFinder[F],
    commitToEventLog: CommitToEventLog[F],
    clock:            java.time.Clock = java.time.Clock.systemUTC()
) extends MissingCommitEventCreator[F] {

  import commitInfoFinder._

  override def createCommits(project: Project, commitsToCreate: List[CommitId])(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[SynchronizationSummary] = for {
    commitInfos <- commitsToCreate.map(findCommitInfo(project.id, _)).sequence
    results     <- commitInfos.map(commitToEventLog.storeCommitInEventLog(project, _, BatchDate(clock))).sequence
    summary <- results
                 .foldLeft(SynchronizationSummary())(_.incrementCount(_))
                 .pure[F]
  } yield summary
}

private[eventgeneration] object MissingCommitEventCreator {
  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry]: F[MissingCommitEventCreator[F]] = for {
    commitInfoFinder <- CommitInfoFinder[F]
    commitToEventLog <- CommitToEventLog[F]
  } yield new MissingCommitEventCreatorImpl[F](commitInfoFinder, commitToEventLog)
}
