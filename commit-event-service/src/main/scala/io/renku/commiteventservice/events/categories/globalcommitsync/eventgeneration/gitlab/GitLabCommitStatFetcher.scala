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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab

import cats.Monad
import cats.data.OptionT
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.{GitLabApiUrl, projects}
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.client.{AccessToken, RestClient}
import org.http4s.Method.GET
import org.http4s.Status.{NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

private[globalcommitsync] trait GitLabCommitStatFetcher[Interpretation[_]] {
  def fetchCommitStats(projectId: projects.Id)(implicit
      maybeAccessToken:           Option[AccessToken]
  ): Interpretation[Option[ProjectCommitStats]]
}

private[globalcommitsync] class GitLabCommitStatFetcherImpl[Interpretation[_]: ConcurrentEffect: Timer: Monad](
    gitLabCommitFetcher:     GitLabCommitFetcher[Interpretation],
    gitLabApiUrl:            GitLabApiUrl,
    gitLabThrottler:         Throttler[Interpretation, GitLab],
    logger:                  Logger[Interpretation],
    retryInterval:           FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride:  Option[Duration] = None
)(implicit executionContext: ExecutionContext)
    extends RestClient(gitLabThrottler,
                       logger,
                       retryInterval = retryInterval,
                       maxRetries = maxRetries,
                       requestTimeoutOverride = requestTimeoutOverride
    )
    with GitLabCommitStatFetcher[Interpretation] {

  import gitLabCommitFetcher._

  override def fetchCommitStats(projectId: projects.Id)(implicit
      maybeAccessToken:                    Option[AccessToken]
  ): Interpretation[Option[ProjectCommitStats]] = (for {
    maybeLatestCommitId <- OptionT.liftF(fetchLatestGitLabCommit(projectId))
    commitCount         <- OptionT(fetchCommitCount(projectId))
  } yield ProjectCommitStats(maybeLatestCommitId, commitCount)).value

  private def fetchCommitCount(projectId: projects.Id)(implicit maybeAccessToken: Option[AccessToken]) = for {
    uri         <- validateUri(s"$gitLabApiUrl/projects/$projectId?statistics=true")
    commitCount <- send(request(GET, uri, maybeAccessToken))(mapCountResponse)
  } yield commitCount

  private implicit lazy val mapCountResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[
        Option[CommitCount]
      ]] = {
    case (Ok, _, response)    => response.as[CommitCount].map(_.some)
    case (NotFound, _, _)     => Option.empty[CommitCount].pure[Interpretation]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitCountDecoder: EntityDecoder[Interpretation, CommitCount] = {
    implicit val commitDecoder: Decoder[CommitCount] =
      _.downField("statistics").downField("commit_count").as[Int].map(CommitCount(_))
    jsonOf[Interpretation, CommitCount]
  }
}

private[globalcommitsync] object GitLabCommitStatFetcher {
  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:      ExecutionContext,
      contextShift:          ContextShift[IO],
      timer:                 Timer[IO]
  ): IO[GitLabCommitStatFetcher[IO]] = for {
    gitLabCommitFetcher <- GitLabCommitFetcher(gitLabThrottler, logger)
    gitLabUrl           <- GitLabUrlLoader[IO]()
  } yield new GitLabCommitStatFetcherImpl[IO](gitLabCommitFetcher, gitLabUrl.apiV4, gitLabThrottler, logger)
}
