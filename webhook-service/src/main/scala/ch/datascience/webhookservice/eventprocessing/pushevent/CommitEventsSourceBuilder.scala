/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.eventprocessing.pushevent

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands._
import ch.datascience.graph.gitlab.GitLab
import ch.datascience.graph.model.events.{CommitEvent, CommitId}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.webhookservice.config.GitLabConfigProvider
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.pushevent.CommitEventsSourceBuilder.EventsFlowBuilder

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class CommitEventsSourceBuilder[Interpretation[_]](
    commitInfoFinder:        CommitInfoFinder[Interpretation],
    eventLogVerifyExistence: EventLogVerifyExistence[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable]) {

  import commitInfoFinder._
  import eventLogVerifyExistence._
  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  def buildEventsSource(pushEvent:        PushEvent,
                        maybeAccessToken: Option[AccessToken]): Interpretation[EventsFlowBuilder[Interpretation]] =
    ME.pure {
      new EventsFlowBuilder[Interpretation] {
        override def transformEventsWith[O](transform: CommitEvent => Interpretation[O]): Interpretation[List[O]] =
          new EventsFlow(pushEvent, maybeAccessToken, transform).run()
      }
    }

  private class EventsFlow[O](pushEvent:        PushEvent,
                              maybeAccessToken: Option[AccessToken],
                              transform:        CommitEvent => Interpretation[O]) {

    def run(): Interpretation[List[O]] = findEventAndTransform(List(pushEvent.commitTo), List.empty)

    private def findEventAndTransform(commitIds: List[CommitId], transformResults: List[O]): Interpretation[List[O]] =
      commitIds match {
        case Nil => ME.pure(transformResults)
        case someCommitIds =>
          for {
            commitEvents            <- findCommitEvents(someCommitIds)
            currentTransformResults <- (commitEvents map transform).sequence
            parentCommitIds         <- ME.pure(commitEvents flatMap (_.parents))
            mergedResults           <- ME.pure(transformResults ++ currentTransformResults)
            newResults              <- findEventAndTransform(parentCommitIds, mergedResults)
          } yield newResults
      }

    private def findCommitEvents(commitIds: List[CommitId]) =
      for {
        validCommitIds           <- ME.pure(commitIds filterNot (_ == DontCareCommitId))
        commitIdsNotPresentInLog <- filterNotInLog(validCommitIds)
        commitEvents             <- (commitIdsNotPresentInLog map findCommitEvent).sequence
      } yield commitEvents

    private def filterNotInLog(commitIds: List[CommitId]) =
      if (commitIds.nonEmpty) filterNotExistingInLog(commitIds, pushEvent.project.id)
      else ME.pure(List.empty[CommitId])

    private def findCommitEvent(commitId: CommitId): Interpretation[CommitEvent] =
      findCommitInfo(pushEvent.project.id, commitId, maybeAccessToken)
        .map(commitInfo => merge(commitInfo, pushEvent))

    private def merge(commitInfo: CommitInfo, pushEvent: PushEvent) = CommitEvent(
      id            = commitInfo.id,
      message       = commitInfo.message,
      committedDate = commitInfo.committedDate,
      author        = commitInfo.author,
      committer     = commitInfo.committer,
      parents       = commitInfo.parents.filterNot(_ == DontCareCommitId),
      project       = pushEvent.project
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
    gitLabThrottler:         Throttler[IO, GitLab]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends CommitEventsSourceBuilder[IO](
      new IOCommitInfoFinder(new GitLabConfigProvider(), gitLabThrottler, ApplicationLogger),
      new IOEventLogVerifyExistence(transactor)
    )
