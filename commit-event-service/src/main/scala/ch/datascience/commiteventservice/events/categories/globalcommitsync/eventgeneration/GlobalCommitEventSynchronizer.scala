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
import cats.kernel.Semigroup
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.common.UpdateResult
import ch.datascience.commiteventservice.events.categories.common.UpdateResult.Failed
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary.SummaryKey
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab.{GitLabCommitFetcher, GitLabCommitStatFetcher}
import ch.datascience.commiteventservice.events.categories.globalcommitsync.{GlobalCommitSyncEvent, _}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.Project
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.AccessTokenFinder._
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[globalcommitsync] trait GlobalCommitEventSynchronizer[Interpretation[_]] {
  def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit]
}
private[globalcommitsync] class GlobalCommitEventSynchronizerImpl[Interpretation[_]: MonadThrow](
    accessTokenFinder:         AccessTokenFinder[Interpretation],
    gitLabCommitStatFetcher:   GitLabCommitStatFetcher[Interpretation],
    gitLabCommitFetcher:       GitLabCommitFetcher[Interpretation],
    commitEventDeleter:        CommitEventDeleter[Interpretation],
    missingCommitEventCreator: MissingCommitEventCreator[Interpretation],
    executionTimeRecorder:     ExecutionTimeRecorder[Interpretation],
    logger:                    Logger[Interpretation]
) extends GlobalCommitEventSynchronizer[Interpretation] {

  import accessTokenFinder._
  import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary._
  import commitEventDeleter._
  import executionTimeRecorder._
  import gitLabCommitFetcher._
  import gitLabCommitStatFetcher._
  import missingCommitEventCreator._

  override def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit] = (for {
    maybeAccessToken <- findAccessToken(event.project.id)
    commitStats      <- fetchCommitStats(event.project.id)(maybeAccessToken)
    commitsInSync    <- commitsInSync(event, commitStats).pure[Interpretation]
    _ <- if (!commitsInSync) {
           syncCommitsAndLogSummary(event)(maybeAccessToken)
         } else measureExecutionTime(SynchronizationSummary().pure[Interpretation]) >>= logSummary(event.project)
  } yield ()).recoverWith { case NonFatal(error) =>
    logger.error(error)(s"$categoryName - Failed to sync commits for project ${event.project}")
    error.raiseError[Interpretation, Unit]
  }

  private def commitsInSync(event: GlobalCommitSyncEvent, commitStats: ProjectCommitStats): Boolean =
    event.commits.length == commitStats.commitCount.value && event.commits.headOption == commitStats.maybeLatestCommit

  private def syncCommitsAndLogSummary(
      event:                   GlobalCommitSyncEvent
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[Unit] =
    measureExecutionTime(syncCommits(event)) >>= logSummary(event.project)

  private def syncCommits(
      event:                   GlobalCommitSyncEvent
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[SynchronizationSummary] = for {
    commitsInGL     <- fetchGitLabCommits(event.project.id)(maybeAccessToken)
    deletionSummary <- deleteExtraneousCommits(event.project, event.commits.filterNot(commitsInGL.contains(_)))
    creationSummary <- createMissingCommits(event.project, commitsInGL.filterNot(event.commits.contains(_)))
  } yield deletionSummary combine creationSummary

  private def logSummary(
      project: Project
  ): ((ElapsedTime, SynchronizationSummary)) => Interpretation[Unit] = { case (elapsedTime, summary) =>
    logger.info(
      s"$categoryName: projectId = ${project.id}, projectPath = ${project.path} -> events generation result: ${summary
        .getSummary()} in ${elapsedTime}ms"
    )
  }

  private def logMessageFor(project: Project, message: String) =
    s"$categoryName: projectId = ${project.id}, projectPath = ${project.path} -> $message"
}

private[globalcommitsync] object GlobalCommitEventSynchronizer {
  def apply(gitLabThrottler:       Throttler[IO, GitLab],
            executionTimeRecorder: ExecutionTimeRecorder[IO],
            logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GlobalCommitEventSynchronizer[IO]] = for {
    accessTokenFinder         <- AccessTokenFinder(logger)
    gitLabCommitStatFetcher   <- GitLabCommitStatFetcher(gitLabThrottler, logger)
    gitLabCommitFetcher       <- GitLabCommitFetcher(gitLabThrottler, logger)
    commitEventDeleter        <- CommitEventDeleter(logger)
    missingCommitEventCreator <- MissingCommitEventCreator(gitLabThrottler, logger)
  } yield new GlobalCommitEventSynchronizerImpl(
    accessTokenFinder,
    gitLabCommitStatFetcher,
    gitLabCommitFetcher,
    commitEventDeleter,
    missingCommitEventCreator,
    executionTimeRecorder,
    logger
  )

  final case class SynchronizationSummary(private val summary: Map[SummaryKey, Int]) {
    import SynchronizationSummary._
    def getSummary(): String =
      s"${get("Created")} created, ${get("Existed")} existed, ${get("Skipped")} skipped, ${get("Deleted")} deleted, ${get("Failed")} failed"

    def get(key: String) = summary.getOrElse(key, 0)

    def updated(result: UpdateResult, newValue: Int): SynchronizationSummary = {
      val newSummary = summary.updated(toSummaryKey(result), newValue)
      new SynchronizationSummary(newSummary)
    }
  }

  object SynchronizationSummary {

    def apply() = new SynchronizationSummary(Map.empty[SummaryKey, Int])

    implicit val semigroup: Semigroup[SynchronizationSummary] =
      (x: SynchronizationSummary, y: SynchronizationSummary) => new SynchronizationSummary(x.summary combine y.summary)

    type SummaryKey = String

    def toSummaryKey(result: UpdateResult): SummaryKey = result match {
      case Failed(_, _) => "Failed"
      case s            => s.toString
    }
  }
}
