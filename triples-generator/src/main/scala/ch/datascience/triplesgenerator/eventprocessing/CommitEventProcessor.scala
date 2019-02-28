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

import cats.MonadError
import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.graph.tokenrepository.TokenRepositoryUrlProvider
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.triplesgenerator.config.FusekiConfigProvider
import ch.datascience.triplesgenerator.eventprocessing.Commands.GitLabRepoUrlFinder
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class CommitEventProcessor[Interpretation[_]](
    commitEventsDeserialiser: CommitEventsDeserialiser[Interpretation],
    accessTokenFinder:        AccessTokenFinder[Interpretation],
    triplesFinder:            TriplesFinder[Interpretation],
    fusekiConnector:          FusekiConnector[Interpretation],
    logger:                   Logger[Interpretation]
)(implicit ME:                MonadError[Interpretation, Throwable])
    extends EventProcessor[Interpretation] {

  import accessTokenFinder._
  import commitEventsDeserialiser._
  import fusekiConnector._
  import triplesFinder._

  def apply(eventJson: String): Interpretation[Unit] = {
    for {
      commits          <- deserialiseToCommitEvents(eventJson)
      maybeAccessToken <- findAccessToken(commits.head.project.id) flatMap logIfNoAccessToken(commits.head)
      _                <- commits.map(commit => toTriplesAndUpload(commit, maybeAccessToken)).sequence
      _                <- logEventProcessed(commits)
    } yield ()
  } recoverWith logDeserialisationError

  private def logIfNoAccessToken(commit: Commit): Option[AccessToken] => Interpretation[Option[AccessToken]] = {
    case found @ Some(_) => ME.pure(found)
    case notFound =>
      logger.info(s"${logMessageCommon(commit)} no access token found so assuming public project")
      ME.pure(notFound)
  }

  private def toTriplesAndUpload(commit: Commit, maybeAccessToken: Option[AccessToken]): Interpretation[Unit] = {
    for {
      triples <- generateTriples(commit, maybeAccessToken)
      result  <- upload(triples)
    } yield result
  } recoverWith logError(commit)

  private def logError(commit: Commit): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"${logMessageCommon(commit)} failed")
  }

  private def logEventProcessed(commits: NonEmptyList[Commit]): Interpretation[_] =
    (commits map { commit =>
      logger.info(s"${logMessageCommon(commit)} processed")
    }).sequence

  private lazy val logMessageCommon: Commit => String = {
    case CommitWithoutParent(id, project) =>
      s"Commit Event id: $id, project: ${project.id} ${project.path}"
    case CommitWithParent(id, parentId, project) =>
      s"Commit Event id: $id, project: ${project.id} ${project.path}, parentId: $parentId"
  }

  private lazy val logDeserialisationError: PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) => logger.error(exception)("Commit Event deserialisation failed")
  }
}

class IOCommitEventProcessor(implicit contextShift: ContextShift[IO], executionContext: ExecutionContext)
    extends CommitEventProcessor[IO](
      new CommitEventsDeserialiser[IO](),
      new IOAccessTokenFinder(new TokenRepositoryUrlProvider[IO]()),
      new IOTriplesFinder(new GitLabRepoUrlFinder[IO](new GitLabUrlProvider[IO]())),
      new IOFusekiConnector(new FusekiConfigProvider[IO]()),
      ApplicationLogger
    )
