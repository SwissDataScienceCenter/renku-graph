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

package ch.datascience.commiteventservice.events.categories.commitsync
package eventgeneration

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.CommitToEventLog
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.events.CommitId
import ch.datascience.http.client.AccessToken
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[commitsync] trait MissedEventsGenerator[Interpretation[_]] {
  def generateMissedEvents(project:          CommitProject,
                           latestCommitId:   CommitId,
                           maybeAccessToken: Option[AccessToken]
  ): Interpretation[UpdateResult]
}

private class MissedEventsGeneratorImpl[Interpretation[_]: MonadThrow](
    commitToEventLog: CommitToEventLog[Interpretation]
) extends MissedEventsGenerator[Interpretation] {

  import UpdateResult._
  import commitToEventLog._

  def generateMissedEvents(project:          CommitProject,
                           latestCommitId:   CommitId,
                           maybeAccessToken: Option[AccessToken]
  ): Interpretation[UpdateResult] =
    addEventsIfMissing(project, latestCommitId, maybeAccessToken) recoverWith toUpdateResult

  private def addEventsIfMissing(project:          CommitProject,
                                 latestCommitId:   CommitId,
                                 maybeAccessToken: Option[AccessToken]
  ): Interpretation[UpdateResult] =
    for {
      startCommit         <- startCommitFrom(project, latestCommitId)
      maybeCreationResult <- storeCommitsInEventLog(startCommit, maybeAccessToken)
    } yield maybeCreationResult

  private def startCommitFrom(project: CommitProject, commitId: CommitId) = StartCommit(
    id = commitId,
    project = Project(project.id, project.path)
  ).pure[Interpretation]

  private lazy val toUpdateResult: PartialFunction[Throwable, Interpretation[UpdateResult]] = {
    case NonFatal(exception) =>
      Failed("event generation failed", exception).pure[Interpretation].widen[UpdateResult]
  }

}

private[commitsync] object MissedEventsGenerator {
  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      timer:            Timer[IO],
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext
  ): IO[MissedEventsGenerator[IO]] =
    for {
      commitToEventLog <- CommitToEventLog(gitLabThrottler, logger)
    } yield new MissedEventsGeneratorImpl(commitToEventLog)
}
