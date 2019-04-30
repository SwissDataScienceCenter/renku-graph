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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{Bracket, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands.{EventLogMarkDone, EventLogMarkFailed}
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, TokenRepositoryUrlProvider}
import ch.datascience.triplesgenerator.eventprocessing.Commands.GitLabRepoUrlFinder

import scala.util.Try

private class TryCommitEventsDeserialiser   extends CommitEventsDeserialiser[Try]
private abstract class TryAccessTokenFinder extends AccessTokenFinder[Try]
private abstract class TryTriplesFinder     extends TriplesFinder[Try]
private abstract class TryFusekiConnector   extends FusekiConnector[Try]
private abstract class TryGitLabUrlProvider extends GitLabUrlProvider[Try]
private abstract class TryEventLogMarkDone(
    transactor: DbTransactor[Try, EventLogDB]
)(implicit ME:  Bracket[Try, Throwable])
    extends EventLogMarkDone[Try](transactor)
private abstract class TryEventLogMarkFailed(
    transactor: DbTransactor[Try, EventLogDB]
)(implicit ME:  Bracket[Try, Throwable])
    extends EventLogMarkFailed[Try](transactor)

private class IOGitLabRepoUrlFinder(
    gitLabUrlProvider: GitLabUrlProvider[IO]
) extends GitLabRepoUrlFinder[IO](gitLabUrlProvider)

abstract class IOEventProcessorRunner(
    eventProcessor: EventProcessor[IO]
) extends EventProcessorRunner[IO](eventProcessor)

private class IOTokenRepositoryUrlProvider extends TokenRepositoryUrlProvider[IO]
