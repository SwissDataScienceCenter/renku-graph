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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration
package gitlab

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{GitLabApiUrl, projects}
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.http.rest.paging.model.Page
import org.http4s.Method.GET
import org.http4s.Status.{NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

private[globalcommitsync] trait GitLabCommitFetcher[F[_]] {

  def fetchLatestGitLabCommit(projectId: projects.Id)(implicit
      maybeAccessToken:                  Option[AccessToken]
  ): F[Option[CommitId]]

  def fetchGitLabCommits(projectId: projects.Id, page: Page)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): F[PageResult]
}

private[globalcommitsync] class GitLabCommitFetcherImpl[F[_]: Async: Logger](
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
    with GitLabCommitFetcher[F] {

  override def fetchLatestGitLabCommit(projectId: projects.Id)(implicit
      maybeAccessToken:                           Option[AccessToken]
  ): F[Option[CommitId]] = for {
    uriString   <- s"$gitLabApiUrl/projects/$projectId/repository/commits".pure[F]
    uri         <- validateUri(uriString).map(_.withQueryParam("per_page", "1"))
    maybeCommit <- send(request(GET, uri, maybeAccessToken))(mapSingleCommitResponse)
  } yield maybeCommit

  override def fetchGitLabCommits(
      projectId:               projects.Id,
      page:                    Page
  )(implicit maybeAccessToken: Option[AccessToken]): F[PageResult] = for {
    uri        <- createUrl(projectId, page)
    pageResult <- send(request(GET, uri, maybeAccessToken))(mapCommitResponse)
  } yield pageResult

  private def createUrl(projectId: projects.Id, page: Page) =
    validateUri(s"$gitLabApiUrl/projects/$projectId/repository/commits").map(_.withQueryParam("page", page.show))

  private implicit lazy val mapCommitResponse: PartialFunction[(Status, Request[F], Response[F]), F[PageResult]] = {
    case (Ok, _, response)    => (response.as[List[CommitId]] -> maybeNextPage(response)).mapN(PageResult(_, _))
    case (NotFound, _, _)     => PageResult.empty.pure[F]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit lazy val mapSingleCommitResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[Option[CommitId]]] = {
    case (Ok, _, response)    => response.as[List[CommitId]].map(_.headOption)
    case (NotFound, _, _)     => Option.empty[CommitId].pure[F]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitIdDecoder: EntityDecoder[F, List[CommitId]] =
    jsonOf[F, List[CommitInfo]].map(_.map(_.id))

  private def maybeNextPage(response: Response[F]): F[Option[Page]] =
    response.headers
      .get(ci"X-Next-Page")
      .flatMap(_.head.value.toIntOption)
      .map(Page.from)
      .map(MonadThrow[F].fromEither(_))
      .sequence
}

private[globalcommitsync] object GitLabCommitFetcher {
  def apply[F[_]: Async: Logger](gitLabThrottler: Throttler[F, GitLab]): F[GitLabCommitFetcher[F]] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
  } yield new GitLabCommitFetcherImpl[F](gitLabUrl.apiV4, gitLabThrottler)
}
