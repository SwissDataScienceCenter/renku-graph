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
import cats.effect.Async
import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.commiteventservice.events.categories.common.SynchronizationSummary
import io.renku.commiteventservice.events.categories.globalcommitsync._
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab.{GitLabCommitFetcher, GitLabCommitStatFetcher}
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder._
import io.renku.http.client.AccessToken
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[globalcommitsync] trait GlobalCommitEventSynchronizer[F[_]] {
  def synchronizeEvents(event: GlobalCommitSyncEvent): F[Unit]
}

private[globalcommitsync] class GlobalCommitEventSynchronizerImpl[F[_]: MonadThrow: Logger](
    accessTokenFinder:         AccessTokenFinder[F],
    gitLabCommitStatFetcher:   GitLabCommitStatFetcher[F],
    gitLabCommitFetcher:       GitLabCommitFetcher[F],
    commitEventDeleter:        CommitEventDeleter[F],
    missingCommitEventCreator: MissingCommitEventCreator[F],
    executionTimeRecorder:     ExecutionTimeRecorder[F]
) extends GlobalCommitEventSynchronizer[F] {

  import accessTokenFinder._
  import commitEventDeleter._
  import executionTimeRecorder._
  import gitLabCommitFetcher._
  import gitLabCommitStatFetcher._
  import missingCommitEventCreator._

  override def synchronizeEvents(event: GlobalCommitSyncEvent): F[Unit] = {
    for {
      maybeAccessToken <- findAccessToken(event.project.id)
      maybeCommitStats <- fetchCommitStats(event.project.id)(maybeAccessToken)
      _                <- syncOrDeleteCommits(event, maybeCommitStats)(maybeAccessToken)
    } yield ()
  }.recoverWith { case NonFatal(error) =>
    Logger[F].error(error)(s"$categoryName - Failed to sync commits for project ${event.project}") >>
      error.raiseError[F, Unit]
  }

  private def syncOrDeleteCommits(event: GlobalCommitSyncEvent, maybeCommitStats: Option[ProjectCommitStats])(implicit
      maybeAccessToken:                  Option[AccessToken]
  ): F[Unit] = maybeCommitStats match {
    case Some(commitStats) =>
      val commitsInSync = event.commits.length == commitStats.commitCount.value &&
        event.commits.headOption == commitStats.maybeLatestCommit

      if (!commitsInSync) syncCommitsAndLogSummary(event)(maybeAccessToken)
      else measureExecutionTime(SynchronizationSummary().pure[F]) >>= logSummary(event.project)
    case None =>
      measureExecutionTime(deleteExtraneousCommits(event.project, event.commits)) >>= logSummary(event.project)
  }

  private def syncCommitsAndLogSummary(
      event:                   GlobalCommitSyncEvent
  )(implicit maybeAccessToken: Option[AccessToken]): F[Unit] =
    measureExecutionTime(syncCommits(event)) >>= logSummary(event.project)

  private def syncCommits(
      event:                   GlobalCommitSyncEvent
  )(implicit maybeAccessToken: Option[AccessToken]): F[SynchronizationSummary] = for {
    commitsInGL     <- fetchGitLabCommits(event.project.id)
    deletionSummary <- deleteExtraneousCommits(event.project, event.commits.filterNot(commitsInGL.contains))
    creationSummary <- createMissingCommits(event.project, commitsInGL.filterNot(event.commits.contains))
  } yield deletionSummary combine creationSummary

  private def logSummary(
      project: Project
  ): ((ElapsedTime, SynchronizationSummary)) => F[Unit] = { case (elapsedTime, summary) =>
    Logger[F].info(show"$categoryName: $project -> events generation result: $summary in ${elapsedTime}ms")
  }
}

private[globalcommitsync] object GlobalCommitEventSynchronizer {
  def apply[F[_]: Async: Temporal: Logger](gitLabThrottler: Throttler[F, GitLab],
                                           executionTimeRecorder: ExecutionTimeRecorder[F]
  ): F[GlobalCommitEventSynchronizer[F]] = for {
    accessTokenFinder         <- AccessTokenFinder[F]
    gitLabCommitStatFetcher   <- GitLabCommitStatFetcher(gitLabThrottler)
    gitLabCommitFetcher       <- GitLabCommitFetcher(gitLabThrottler)
    commitEventDeleter        <- CommitEventDeleter[F]
    missingCommitEventCreator <- MissingCommitEventCreator(gitLabThrottler)
  } yield new GlobalCommitEventSynchronizerImpl(
    accessTokenFinder,
    gitLabCommitStatFetcher,
    gitLabCommitFetcher,
    commitEventDeleter,
    missingCommitEventCreator,
    executionTimeRecorder
  )
}
