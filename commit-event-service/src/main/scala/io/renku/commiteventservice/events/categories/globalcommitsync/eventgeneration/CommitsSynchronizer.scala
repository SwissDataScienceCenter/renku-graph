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

import cats.effect.Async
import cats.syntax.all._
import cats.{NonEmptyParallel, Show}
import io.renku.commiteventservice.events.categories.common.SynchronizationSummary.semigroup
import io.renku.commiteventservice.events.categories.common.{SynchronizationSummary, UpdateResult}
import io.renku.commiteventservice.events.categories.globalcommitsync._
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab.{GitLabCommitFetcher, GitLabCommitStatFetcher}
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.graph.model.events.CommitId
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder._
import io.renku.http.client.AccessToken
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger
import io.renku.http.client.GitLabClient

import scala.util.control.NonFatal

private[globalcommitsync] trait CommitsSynchronizer[F[_]] {
  def synchronizeEvents(event: GlobalCommitSyncEvent): F[Unit]
}

private[globalcommitsync] class CommitsSynchronizerImpl[F[_]: Async: NonEmptyParallel: Logger](
    accessTokenFinder:         AccessTokenFinder[F],
    gitLabCommitStatFetcher:   GitLabCommitStatFetcher[F],
    gitLabCommitFetcher:       GitLabCommitFetcher[F],
    elCommitFetcher:           ELCommitFetcher[F],
    commitEventDeleter:        CommitEventDeleter[F],
    missingCommitEventCreator: MissingCommitEventCreator[F],
    executionTimeRecorder:     ExecutionTimeRecorder[F]
) extends CommitsSynchronizer[F] {

  val commitsPerPage = PerPage(50)

  import accessTokenFinder._
  import commitEventDeleter._
  import elCommitFetcher._
  import executionTimeRecorder._
  import gitLabCommitFetcher._
  import gitLabCommitStatFetcher._
  import missingCommitEventCreator._

  override def synchronizeEvents(event: GlobalCommitSyncEvent): F[Unit] = {
    findAccessToken(event.project.id) >>= { implicit maybeAccessToken =>
      fetchCommitStats(event.project.id) >>= {
        case maybeStats if outOfSync(event, maybeStats) => createOrDeleteEvents(event)
        case _ =>
          logSummary(event.project,
                     SynchronizationSummary().updated(UpdateResult.Skipped, event.commits.count.value.toInt)
          )
      }
    }
  } recoverWith { case NonFatal(error) =>
    Logger[F].error(error)(show"$categoryName - failed to sync commits for project ${event.project}") >>
      error.raiseError[F, Unit]
  }

  private def outOfSync(event: GlobalCommitSyncEvent, maybeCommitStats: Option[ProjectCommitStats]) =
    maybeCommitStats match {
      case Some(commitStats) =>
        event.commits.count != commitStats.commitsCount ||
          !commitStats.maybeLatestCommit.contains(event.commits.latest)
      case _ => true
    }

  private def createOrDeleteEvents(
      event:                   GlobalCommitSyncEvent
  )(implicit maybeAccessToken: Option[AccessToken]): F[Unit] = measureExecutionTime {
    buildActionsList(event) >>= execute(event.project)
  } >>= logSummary(event.project)

  private def buildActionsList(
      event:                   GlobalCommitSyncEvent,
      maybeNextGLPage:         Option[Page] = Some(Page.first),
      maybeNextELPage:         Option[Page] = Some(Page.first),
      actions:                 Map[Action, List[CommitId]] = Map(Create -> Nil, Delete -> Nil)
  )(implicit maybeAccessToken: Option[AccessToken]): F[Map[Action, List[CommitId]]] =
    (
      fetch(maybeNextGLPage, fetchGitLabCommits(event.project.id, _)),
      fetch(maybeNextELPage, fetchELCommits(event.project.path, _))
    ).parMapN { case (glCommitsPage, elCommitsPage) =>
      addNextPage(update(actions, glCommitsPage, elCommitsPage), glCommitsPage, elCommitsPage, event)
    }.flatten

  private def fetch(maybeNextPage: Option[Page], fetcher: PagingRequest => F[PageResult]) =
    maybeNextPage
      .map(page => fetcher(PagingRequest(page, commitsPerPage)))
      .getOrElse(PageResult.empty.pure[F])

  private def update(actions: Map[Action, List[CommitId]], glCommitsPage: PageResult, elCommitsPage: PageResult) = {
    val deletions = elCommitsPage.commits diff glCommitsPage.commits
    val creations = glCommitsPage.commits diff elCommitsPage.commits

    val (updatedCommitsToCreate, updatedDeletions) = removeCommitsNotRequiringAction(actions(Create), deletions)
    val (updatedCommitsToDelete, updatedCreations) = removeCommitsNotRequiringAction(actions(Delete), creations)

    Map(Create -> (updatedCommitsToCreate ::: updatedCreations).distinct,
        Delete -> (updatedCommitsToDelete ::: updatedDeletions).distinct
    )
  }

  private def removeCommitsNotRequiringAction(allCommits: List[CommitId], candidateCommits: List[CommitId]) = {
    val toBeSkipped       = allCommits intersect candidateCommits
    val updatedAllCommits = allCommits diff toBeSkipped
    updatedAllCommits -> (candidateCommits diff toBeSkipped)
  }

  private def addNextPage(actions:       Map[Action, List[CommitId]],
                          glCommitsPage: PageResult,
                          elCommitsPage: PageResult,
                          event:         GlobalCommitSyncEvent
  )(implicit maybeAccessToken:           Option[AccessToken]) =
    if (glCommitsPage.maybeNextPage.isEmpty && elCommitsPage.maybeNextPage.isEmpty) actions.pure[F]
    else buildActionsList(event, glCommitsPage.maybeNextPage, elCommitsPage.maybeNextPage, actions)

  private def execute(project: Project)(actions: Map[Action, List[CommitId]])(implicit
      maybeAccessToken:        Option[AccessToken]
  ): F[SynchronizationSummary] =
    actions
      .map {
        case (Create, commits) => createCommits(project, commits)
        case (Delete, commits) => deleteCommits(project, commits)
      }
      .toList
      .sequence
      .map(_.reduce(_ |+| _))

  private sealed trait Action extends Product with Serializable
  private case object Create  extends Action
  private case object Delete  extends Action

  private def logSummary(project: Project): ((ElapsedTime, SynchronizationSummary)) => F[Unit] = {
    case (elapsedTime, summary) => logSummary(project, summary, elapsedTime.some)
  }

  private def logSummary(
      project:          Project,
      summary:          SynchronizationSummary,
      maybeElapsedTime: Option[ElapsedTime] = None
  ) = Logger[F].info(show"$categoryName: $project -> events generation result: $summary$maybeElapsedTime")

  private implicit lazy val maybeElapsedShow: Show[Option[ElapsedTime]] = Show.show {
    case Some(time) => show" in ${time}ms"
    case _          => ""
  }
}

private[globalcommitsync] object CommitsSynchronizer {
  def apply[F[_]: Async: NonEmptyParallel: Logger](gitLabClient: GitLabClient[F],
                                                   gitLabThrottler:       Throttler[F, GitLab],
                                                   executionTimeRecorder: ExecutionTimeRecorder[F]
  ): F[CommitsSynchronizer[F]] = for {
    accessTokenFinder         <- AccessTokenFinder[F]
    gitLabCommitStatFetcher   <- GitLabCommitStatFetcher(gitLabClient, gitLabThrottler)
    gitLabCommitFetcher       <- GitLabCommitFetcher[F](gitLabClient)
    elCommitFetcher           <- ELCommitFetcher[F]
    commitEventDeleter        <- CommitEventDeleter[F]
    missingCommitEventCreator <- MissingCommitEventCreator(gitLabThrottler)
  } yield new CommitsSynchronizerImpl(
    accessTokenFinder,
    gitLabCommitStatFetcher,
    gitLabCommitFetcher,
    elCommitFetcher,
    commitEventDeleter,
    missingCommitEventCreator,
    executionTimeRecorder
  )
}
