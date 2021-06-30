package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import ch.datascience.commiteventservice.events.categories.common.CommitInfo
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects
import ch.datascience.http.client.{AccessToken, RestClient}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import org.typelevel.log4cats.Logger
import org.http4s.Method.GET
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.dsl.io._
import org.http4s.dsl.request
import org.http4s.util.CaseInsensitiveString
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.tinytypes.json.TinyTypeDecoders.stringDecoder
import org.http4s.Status.{Ok, Unauthorized}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

trait GitLabCommitFetcher[Interpretation[_]] {
  def fetchCommitStats(projectId: projects.Id)(implicit
      maybeAccessToken:           Option[AccessToken]
  ): Interpretation[ProjectCommitStats]
  def fetchAllGitLabCommits(projectId: projects.Id)(implicit
      maybeAccessToken:                Option[AccessToken]
  ): Interpretation[List[CommitId]]
}

private class GitLabCommitFetcherImpl[Interpretation[_]: ConcurrentEffect: Timer](
    gitLabUrl:               GitLabUrl,
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
  override def fetchAllGitLabCommits(projectId: projects.Id)(implicit
      maybeAccessToken:                         Option[AccessToken]
  ): Interpretation[List[CommitId]] = ???

  override def fetchCommitStats(projectId: projects.Id)(implicit
      maybeAccessToken:                    Option[AccessToken]
  ): Interpretation[ProjectCommitStats] = for {
    maybeLatestCommitId <- fetchLatestCommit(projectId)
    commitCount         <- fetchCommitCount(projectId)
  } yield ProjectCommitStats(maybeLatestCommitId, commitCount)

  private def fetchCommitCount(projectId: projects.Id)(implicit maybeAccessToken: Option[AccessToken]) = for {
    uri         <- validateUri(s"${gitLabUrl.apiV4}/projects/$projectId")
    commitCount <- send(request(GET, uri, maybeAccessToken))(mapCountResponse)
  } yield commitCount

  private implicit lazy val mapCountResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[CommitCount]] = {
    case (Ok, _, response)    => response.as[CommitCount]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitCountDecoder: EntityDecoder[Interpretation, CommitCount] =
    jsonOf[Interpretation, CommitCount]

  private def fetchLatestCommit(
      projectId:               projects.Id
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[Option[CommitId]] = for {
    stringUri     <- s"$gitLabUrl/api/v4/projects/$projectId/repository/commits".pure[Interpretation]
    uri           <- validateUri(stringUri) map (_.withQueryParam("per_page", "1"))
    maybeCommitId <- send(request(GET, uri, maybeAccessToken))(mapCommitResponse)
  } yield maybeCommitId

  private implicit lazy val mapCommitResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]),
                        Interpretation[Option[CommitId]]
      ] = {
    case (Ok, _, response)    => response.as[Option[CommitId]]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitIdDecoder: EntityDecoder[Interpretation, Option[CommitId]] =
    jsonOf[Interpretation, Option[CommitId]]

}

private object GitLabCommitFetcher {
  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GitLabCommitFetcher[IO]] = for {
    gitLabUrl <- GitLabUrl[IO]()
  } yield new GitLabCommitFetcherImpl[IO](gitLabUrl, gitLabThrottler, logger)
}
