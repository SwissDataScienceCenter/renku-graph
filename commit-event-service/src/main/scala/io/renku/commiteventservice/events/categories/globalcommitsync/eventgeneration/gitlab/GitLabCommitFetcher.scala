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

import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrlLoader
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.{GitLabApiUrl, projects}
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.http.client.{AccessToken, RestClient}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.commiteventservice.events.categories.common.CommitInfo
import org.http4s.Method.GET
import org.http4s.Status.{NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.util.CaseInsensitiveString
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

private[globalcommitsync] trait GitLabCommitFetcher[Interpretation[_]] {

  def fetchLatestGitLabCommit(projectId: projects.Id)(implicit
      maybeAccessToken:                  Option[AccessToken]
  ): Interpretation[Option[CommitId]]

  def fetchGitLabCommits(projectId: projects.Id)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): Interpretation[List[CommitId]]
}

private[globalcommitsync] class GitLabCommitFetcherImpl[Interpretation[_]: ConcurrentEffect: Timer](
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
    with GitLabCommitFetcher[Interpretation] {

  override def fetchLatestGitLabCommit(projectId: projects.Id)(implicit
      maybeAccessToken:                           Option[AccessToken]
  ): Interpretation[Option[CommitId]] = for {
    uriString   <- getUriString(projectId)
    uri         <- validateUri(uriString).map(_.withQueryParam("per_page", "1"))
    maybeCommit <- send(request(GET, uri, maybeAccessToken))(mapSingleCommitResponse)
  } yield maybeCommit

  override def fetchGitLabCommits(projectId: projects.Id)(implicit
      maybeAccessToken:                      Option[AccessToken]
  ): Interpretation[List[CommitId]] = for {
    uri     <- getUriString(projectId)
    commits <- fetch(uri)
  } yield commits

  private def getUriString(projectId: projects.Id): Interpretation[String] =
    s"$gitLabApiUrl/projects/$projectId/repository/commits".pure[Interpretation]

  private def fetch(uriWithoutPage:           String,
                    maybePage:                Option[Int] = None,
                    previouslyFetchedCommits: List[CommitId] = List.empty[CommitId]
  )(implicit
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[List[CommitId]] = for {
    uri          <- addPageToUrl(uriWithoutPage, maybePage)
    responsePair <- send(request(GET, uri, maybeAccessToken))(mapCommitResponse)
    allCommitIds <- addNextPage(uriWithoutPage,
                                previouslyFetchedCommits,
                                newlyFetchedCommits = responsePair._1,
                                maybeNextPage = responsePair._2
                    )
  } yield allCommitIds

  private def addPageToUrl(url: String, maybePage: Option[Int]) = maybePage match {
    case Some(page) => validateUri(s"$url").map(_.withQueryParam("page", page.toString))
    case None       => validateUri(url)
  }

  private implicit lazy val mapCommitResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[
        (List[CommitId], Option[Int])
      ]] = {
    case (Ok, _, response)    => response.as[List[CommitId]].map(commits => commits -> maybeNextPage(response))
    case (NotFound, _, _)     => (List.empty[CommitId] -> Option.empty[Int]).pure[Interpretation]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit lazy val mapSingleCommitResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]),
                        Interpretation[Option[CommitId]]
      ] = {
    case (Ok, _, response)    => response.as[List[CommitId]].map(_.headOption)
    case (NotFound, _, _)     => Option.empty[CommitId].pure[Interpretation]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitIdDecoder: EntityDecoder[Interpretation, List[CommitId]] =
    jsonOf[Interpretation, List[CommitInfo]].map(_.map(_.id))

  private def addNextPage(
      baseUriString:            String,
      previouslyFetchedCommits: List[CommitId],
      newlyFetchedCommits:      List[CommitId],
      maybeNextPage:            Option[Int]
  )(implicit maybeAccessToken:  Option[AccessToken]): Interpretation[List[CommitId]] =
    maybeNextPage match {
      case page @ Some(_) => fetch(baseUriString, page, previouslyFetchedCommits ::: newlyFetchedCommits)
      case None           => (previouslyFetchedCommits ::: newlyFetchedCommits).pure[Interpretation]
    }

  private def maybeNextPage(response: Response[Interpretation]): Option[Int] =
    response.headers.get(CaseInsensitiveString("X-Next-Page")).flatMap(_.value.toIntOption)
}

private[globalcommitsync] object GitLabCommitFetcher {
  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GitLabCommitFetcher[IO]] = for {
    gitLabUrl <- GitLabUrlLoader[IO]()
  } yield new GitLabCommitFetcherImpl[IO](gitLabUrl.apiV4, gitLabThrottler, logger)
}
