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

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.renku.commiteventservice.events.categories.globalcommitsync.CommitsCount
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.{GitLabApiUrl, projects}
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.client.{AccessToken, GitLabClient, RestClient}
import org.http4s.Method.GET
import org.http4s.Status.{NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

private[globalcommitsync] trait GitLabCommitStatFetcher[F[_]] {
  def fetchCommitStats(projectId: projects.Id)(implicit
      maybeAccessToken:           Option[AccessToken]
  ): F[Option[ProjectCommitStats]]
}

private[globalcommitsync] class GitLabCommitStatFetcherImpl[F[_]: Async: Logger](
    gitLabCommitFetcher:    GitLabCommitFetcher[F],
    gitLabApiUrl:           GitLabApiUrl,
    gitLabThrottler:        Throttler[F, GitLab],
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(gitLabThrottler,
                     retryInterval = retryInterval,
                     maxRetries = maxRetries,
                     requestTimeoutOverride = requestTimeoutOverride
    )
    with GitLabCommitStatFetcher[F] {

  import gitLabCommitFetcher._

  override def fetchCommitStats(projectId: projects.Id)(implicit
      maybeAccessToken:                    Option[AccessToken]
  ): F[Option[ProjectCommitStats]] = {
    for {
      maybeLatestCommitId <- OptionT.liftF(fetchLatestGitLabCommit(projectId))
      commitCount         <- OptionT(fetchCommitCount(projectId))
    } yield ProjectCommitStats(maybeLatestCommitId, commitCount)
  }.value

  private def fetchCommitCount(projectId: projects.Id)(implicit maybeAccessToken: Option[AccessToken]) = for {
    uri         <- validateUri(s"$gitLabApiUrl/projects/$projectId?statistics=true")
    commitCount <- send(request(GET, uri, maybeAccessToken))(mapCountResponse)
  } yield commitCount

  private implicit lazy val mapCountResponse: PartialFunction[(Status, Request[F], Response[F]), F[
    Option[CommitsCount]
  ]] = {
    case (Ok, _, response)    => response.as[CommitsCount].map(_.some)
    case (NotFound, _, _)     => Option.empty[CommitsCount].pure[F]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitCountDecoder: EntityDecoder[F, CommitsCount] = {
    implicit val commitDecoder: Decoder[CommitsCount] =
      _.downField("statistics").downField("commit_count").as(CommitsCount.decoder)
    jsonOf[F, CommitsCount]
  }
}

private[globalcommitsync] object GitLabCommitStatFetcher {
  def apply[F[_]: Async: Logger](gitLabClient: GitLabClient[F],
                                 gitLabThrottler: Throttler[F, GitLab]
  ): F[GitLabCommitStatFetcher[F]] = for {
    gitLabCommitFetcher <- GitLabCommitFetcher(gitLabClient)
    gitLabUrl           <- GitLabUrlLoader[F]()
  } yield new GitLabCommitStatFetcherImpl[F](gitLabCommitFetcher, gitLabUrl.apiV4, gitLabThrottler)
}
