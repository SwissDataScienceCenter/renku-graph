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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.{BatchDate, CommitId}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEvent.{NewCommitEvent, SkippedCommitEvent}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.CommitEventsSourceBuilder.EventsFlowBuilder
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.EventCreationResult.Existed
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.{CommitEvent, CommitInfo, StartCommit}

import java.time.Clock
import scala.concurrent.ExecutionContext

private class CommitEventsSourceBuilder[Interpretation[_]](
    commitInfoFinder: CommitInfoFinder[Interpretation]
)(implicit ME:        MonadError[Interpretation, Throwable]) {

  import commitInfoFinder._
  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  def buildEventsSource(startCommit:      StartCommit,
                        maybeAccessToken: Option[AccessToken],
                        clock:            Clock
  ): Interpretation[EventsFlowBuilder[Interpretation]] = ME.pure {
    transform: Function1[CommitEvent, Interpretation[EventCreationResult]] =>
      new EventsFlow(startCommit, maybeAccessToken, transform, clock).run()
  }

  private class EventsFlow(startCommit:      StartCommit,
                           maybeAccessToken: Option[AccessToken],
                           transform:        CommitEvent => Interpretation[EventCreationResult],
                           clock:            Clock
  ) {

    def run(): Interpretation[List[EventCreationResult]] = findEventAndTransform(startCommit.id, List.empty)

    private def findEventAndTransform(commitId:         CommitId,
                                      transformResults: List[EventCreationResult],
                                      batchDate:        BatchDate = BatchDate(clock)
    ): Interpretation[List[EventCreationResult]] =
      maybeCommitEvent(commitId, batchDate) flatMap {
        case None => transformResults.pure[Interpretation]
        case Some(commitEvent) =>
          for {
            currentTransformResult <- transform(commitEvent)
            mergedResults          <- (transformResults :+ currentTransformResult).pure[Interpretation]
            parentsResults <- if (currentTransformResult == Existed)
                                List.empty[EventCreationResult].pure[Interpretation]
                              else transformParents(commitEvent, batchDate)
          } yield mergedResults ++ parentsResults
      }

    private def maybeCommitEvent(commitId: CommitId, batchDate: BatchDate): Interpretation[Option[CommitEvent]] =
      if (commitId == DontCareCommitId) Option.empty[CommitEvent].pure[Interpretation]
      else findCommitEvent(commitId, batchDate).map(Option.apply)

    private def findCommitEvent(commitId: CommitId, batchDate: BatchDate): Interpretation[CommitEvent] =
      findCommitInfo(startCommit.project.id, commitId, maybeAccessToken) map toCommitEvent(batchDate)

    private def toCommitEvent(batchDate: BatchDate)(commitInfo: CommitInfo) = commitInfo.message.value match {
      case message if message contains "renku migrate" =>
        SkippedCommitEvent(
          id = commitInfo.id,
          message = commitInfo.message,
          committedDate = commitInfo.committedDate,
          author = commitInfo.author,
          committer = commitInfo.committer,
          parents = commitInfo.parents.filterNot(_ == DontCareCommitId),
          project = startCommit.project,
          batchDate = batchDate
        )
      case _ =>
        NewCommitEvent(
          id = commitInfo.id,
          message = commitInfo.message,
          committedDate = commitInfo.committedDate,
          author = commitInfo.author,
          committer = commitInfo.committer,
          parents = commitInfo.parents.filterNot(_ == DontCareCommitId),
          project = startCommit.project,
          batchDate = batchDate
        )
    }

    private def transformParents(commitEvent: CommitEvent, batchDate: BatchDate) =
      for {
        parents                 <- commitEvent.parents.pure[Interpretation]
        parentsTransformResults <- parents.map(findEventAndTransform(_, transformResults = Nil, batchDate)).sequence
      } yield parentsTransformResults.flatten
  }
}

private object CommitEventsSourceBuilder {

  abstract class EventsFlowBuilder[Interpretation[_]] {
    def transformEventsWith(
        transform: CommitEvent => Interpretation[EventCreationResult]
    ): Interpretation[List[EventCreationResult]]
  }

  def apply(
      gitLabThrottler: Throttler[IO, GitLab]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[CommitEventsSourceBuilder[IO]] =
    for {
      gitLabUrl <- GitLabUrl[IO]()
    } yield new CommitEventsSourceBuilder[IO](new CommitInfoFinderImpl(gitLabUrl, gitLabThrottler, ApplicationLogger))
}
