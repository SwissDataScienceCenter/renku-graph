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
import ch.datascience.commiteventservice.events.categories.common.{CommitInfoFinder, CommitToEventLog, UpdateResult}
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary.toSummaryKey
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.{BatchDate, CommitId}
import ch.datascience.http.client.AccessToken
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[eventgeneration] trait MissingCommitEventCreator[Interpretation[_]] {
  def createMissingCommits(project: Project, commitsToCreate: List[CommitId])(implicit
      maybeAccessToken:             Option[AccessToken]
  ): Interpretation[SynchronizationSummary]
}

private[eventgeneration] class MissingCommitEventCreatorImpl[Interpretation[_]: MonadThrow](
    commitInfoFinder: CommitInfoFinder[Interpretation],
    commitToEventLog: CommitToEventLog[Interpretation],
    clock:            java.time.Clock = java.time.Clock.systemUTC()
) extends MissingCommitEventCreator[Interpretation] {

  import commitInfoFinder._

  override def createMissingCommits(project: Project, commitsToCreate: List[CommitId])(implicit
      maybeAccessToken:                      Option[AccessToken]
  ): Interpretation[SynchronizationSummary] = for {
    commitInfos <-
      commitsToCreate.map(findCommitInfo(project.id, _)).sequence
    results <- commitInfos.map(commitToEventLog.storeCommitInEventLog(project, _, BatchDate(clock))).sequence
    summary <- results
                 .foldLeft(SynchronizationSummary()) { (summary, result: UpdateResult) =>
                   val currentCount = summary.get(toSummaryKey(result))
                   summary.updated(result, currentCount + 1)
                 }
                 .pure[Interpretation]
  } yield summary
}

private[eventgeneration] object MissingCommitEventCreator {
  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:      ExecutionContext,
      contextShift:          ContextShift[IO],
      timer:                 Timer[IO]
  ): IO[MissingCommitEventCreator[IO]] = for {
    commitInfoFinder <- CommitInfoFinder(gitLabThrottler, logger)
    commitToEventLog <- CommitToEventLog(logger)
  } yield new MissingCommitEventCreatorImpl[IO](commitInfoFinder, commitToEventLog)
}
