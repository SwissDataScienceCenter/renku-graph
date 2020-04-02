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
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.{BatchDate, CommitId, EventId}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.webhookservice.commits.{CommitInfo, CommitInfoFinder, IOCommitInfoFinder}
import ch.datascience.webhookservice.eventprocessing.startcommit.CommitEventsSourceBuilder.EventsFlowBuilder
import ch.datascience.webhookservice.eventprocessing.{CommitEvent, StartCommit}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class CommitEventsSourceBuilder[Interpretation[_]](
    commitInfoFinder:        CommitInfoFinder[Interpretation],
    eventLogVerifyExistence: EventLogVerifyExistence[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable]) {

  import commitInfoFinder._
  import eventLogVerifyExistence._
  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  def buildEventsSource(startCommit:      StartCommit,
                        maybeAccessToken: Option[AccessToken],
                        clock:            Clock): Interpretation[EventsFlowBuilder[Interpretation]] =
    ME.pure {
      new EventsFlowBuilder[Interpretation] {
        override def transformEventsWith[O](transform: CommitEvent => Interpretation[O]): Interpretation[List[O]] =
          new EventsFlow(startCommit, maybeAccessToken, transform, clock).run()
      }
    }

  private class EventsFlow[O](startCommit:      StartCommit,
                              maybeAccessToken: Option[AccessToken],
                              transform:        CommitEvent => Interpretation[O],
                              clock:            Clock) {

    def run(): Interpretation[List[O]] = findEventAndTransform(List(startCommit.id), List.empty)

    private def findEventAndTransform(commitIds:        List[CommitId],
                                      transformResults: List[O],
                                      batchDate:        BatchDate = BatchDate(clock)): Interpretation[List[O]] =
      commitIds match {
        case Nil => ME.pure(transformResults)
        case someCommitIds =>
          for {
            commitEvents            <- findCommitEvents(someCommitIds, batchDate)
            currentTransformResults <- (commitEvents map transform).sequence
            parentCommitIds         <- ME.pure(commitEvents flatMap (_.parents))
            mergedResults           <- ME.pure(transformResults ++ currentTransformResults)
            newResults              <- findEventAndTransform(parentCommitIds, mergedResults, batchDate)
          } yield newResults
      }

    private def findCommitEvents(commitIds: List[CommitId], batchDate: BatchDate) =
      for {
        validCommitIds           <- ME.pure(commitIds filterNot (_ == DontCareCommitId))
        commitIdsNotPresentInLog <- filterNotInLog(validCommitIds)
        commitEvents             <- (commitIdsNotPresentInLog map findCommitEvent(batchDate)).sequence
      } yield commitEvents

    private def filterNotInLog(commitIds: List[CommitId]) =
      if (commitIds.nonEmpty)
        filterNotExistingInLog(commitIds map toEventId, startCommit.project.id).map(_ map toCommitId)
      else
        ME.pure(List.empty[CommitId])

    private lazy val toEventId:  CommitId => EventId = id => EventId(id.value)
    private lazy val toCommitId: EventId => CommitId = id => CommitId(id.value)

    private def findCommitEvent(batchDate: BatchDate)(commitId: CommitId): Interpretation[CommitEvent] =
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
  }
}

private object CommitEventsSourceBuilder {

  abstract class EventsFlowBuilder[Interpretation[_]] {
    def transformEventsWith[O](transform: CommitEvent => Interpretation[O]): Interpretation[List[O]]
  }
}

private class IOCommitEventsSourceBuilder(
    transactor:              DbTransactor[IO, EventLogDB],
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends CommitEventsSourceBuilder[IO](
      new IOCommitInfoFinder(gitLabUrl, gitLabThrottler, ApplicationLogger),
      new IOEventLogVerifyExistence(transactor)
    )
