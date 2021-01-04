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

package ch.datascience.webhookservice.hookcreation

import cats.MonadError
import cats.effect.{Concurrent, ContextShift, IO}
import ch.datascience.webhookservice.commits.LatestCommitFinder
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.startcommit.CommitToEventLog
import ch.datascience.webhookservice.hookvalidation.HookValidator
import ch.datascience.webhookservice.project.{ProjectHookUrl, ProjectInfoFinder}
import ch.datascience.webhookservice.tokenrepository.AccessTokenAssociator
import io.chrisdavenport.log4cats.Logger

import scala.util.Try

private class TryEventsHistoryLoader(
    latestCommitFinder: LatestCommitFinder[Try],
    commitToEventLog:   CommitToEventLog[Try],
    logger:             Logger[Try]
)(implicit ME:          MonadError[Try, Throwable])
    extends EventsHistoryLoader[Try](latestCommitFinder, commitToEventLog, logger)

private class IOHookCreator(
    projectHookUrl:        ProjectHookUrl,
    projectHookValidator:  HookValidator[IO],
    projectInfoFinder:     ProjectInfoFinder[IO],
    hookTokenCrypto:       HookTokenCrypto[IO],
    projectHookCreator:    ProjectHookCreator[IO],
    accessTokenAssociator: AccessTokenAssociator[IO],
    eventsHistoryLoader:   EventsHistoryLoader[IO],
    logger:                Logger[IO]
)(implicit ME:             MonadError[IO, Throwable], contextShift: ContextShift[IO], concurrent: Concurrent[IO])
    extends HookCreator[IO](
      projectHookUrl,
      projectHookValidator,
      projectInfoFinder,
      hookTokenCrypto,
      projectHookCreator,
      accessTokenAssociator,
      eventsHistoryLoader,
      logger
    )

private class IOEventsHistoryLoader(
    latestCommitFinder: LatestCommitFinder[IO],
    commitToEventLog:   CommitToEventLog[IO],
    logger:             Logger[IO]
)(implicit ME:          MonadError[IO, Throwable])
    extends EventsHistoryLoader(
      latestCommitFinder,
      commitToEventLog,
      logger
    )
