/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.eventprocessing.startcommit

import java.time.Clock

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.{BatchDate, CommitId}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.webhookservice.commits.{CommitInfo, CommitInfoFinder, IOCommitInfoFinder}
import ch.datascience.webhookservice.eventprocessing.startcommit.CommitEventsSourceBuilder.EventsFlowBuilder
import ch.datascience.webhookservice.eventprocessing.{CommitEvent, StartCommit}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class CommitEventsSourceBuilder[Interpretation[_]](
    commitInfoFinder: CommitInfoFinder[Interpretation]
)(implicit ME:        MonadError[Interpretation, Throwable]) {

  import SendingResult._
  import commitInfoFinder._
  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  def buildEventsSource(startCommit:      StartCommit,
                        maybeAccessToken: Option[AccessToken],
                        clock:            Clock): Interpretation[EventsFlowBuilder[Interpretation]] = ME.pure {
    transform: Function1[CommitEvent, Interpretation[SendingResult]] =>
      new EventsFlow(startCommit, maybeAccessToken, transform, clock).run()
  }

  private class EventsFlow(startCommit:      StartCommit,
                           maybeAccessToken: Option[AccessToken],
                           transform:        CommitEvent => Interpretation[SendingResult],
                           clock:            Clock) {

    def run(): Interpretation[List[SendingResult]] = findEventAndTransform(startCommit.id, List.empty)

    private def findEventAndTransform(commitId:         CommitId,
                                      transformResults: List[SendingResult],
                                      batchDate:        BatchDate = BatchDate(clock)): Interpretation[List[SendingResult]] =
      maybeCommitEvent(commitId, batchDate) flatMap {
        case None => transformResults.pure[Interpretation]
        case Some(commitEvent) =>
          for {
            currentTransformResult <- transform(commitEvent)
            mergedResults          <- (transformResults :+ currentTransformResult).pure[Interpretation]
            parentsResults <- if (currentTransformResult == Existed) List.empty[SendingResult].pure[Interpretation]
                             else transformParents(commitEvent, batchDate)
          } yield mergedResults ++ parentsResults
      }

    private def maybeCommitEvent(commitId: CommitId, batchDate: BatchDate): Interpretation[Option[CommitEvent]] =
      if (commitId == DontCareCommitId) Option.empty[CommitEvent].pure[Interpretation]
      else findCommitEvent(commitId, batchDate).map(Option.apply)

    private def findCommitEvent(commitId: CommitId, batchDate: BatchDate): Interpretation[CommitEvent] =
      findCommitInfo(startCommit.project.id, commitId, maybeAccessToken) map toCommitEvent(batchDate)

    private def toCommitEvent(batchDate: BatchDate)(commitInfo: CommitInfo) = CommitEvent(
      id            = commitInfo.id,
      message       = commitInfo.message,
      committedDate = commitInfo.committedDate,
      author        = commitInfo.author,
      committer     = commitInfo.committer,
      parents       = commitInfo.parents.filterNot(_ == DontCareCommitId),
      project       = startCommit.project,
      batchDate     = batchDate
    )

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
        transform: CommitEvent => Interpretation[SendingResult]
    ): Interpretation[List[SendingResult]]
  }
}

private object IOCommitEventsSourceBuilder {
  def apply(
      gitLabThrottler:         Throttler[IO, GitLab]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[CommitEventsSourceBuilder[IO]] =
    for {
      gitLabUrl <- GitLabUrl[IO]()
    } yield new CommitEventsSourceBuilder[IO](new IOCommitInfoFinder(gitLabUrl, gitLabThrottler, ApplicationLogger))
}
