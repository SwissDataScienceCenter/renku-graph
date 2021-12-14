package io.renku.http.client

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.NonNegative
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.GitLabApiUrl
import io.renku.http.client.HttpRequest.NamedRequest
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.metrics.GitLabApiCallRecorder
import org.http4s.{Method, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

trait GitLabClient[F[_]] {

  def send[ResultType](method: Method, path: Uri, endpointName: String Refined NonEmpty)(
      mapResponse:             ResponseMappingF[F, ResultType]
  )(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[ResultType]
}

final class GitLabClientImpl[F[_]: Async: Logger](
    gitLabApiUrl:           GitLabApiUrl,
    apiCallRecorder:        GitLabApiCallRecorder[F],
    gitLabThrottler:        Throttler[F, GitLab],
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(gitLabThrottler,
                     maybeTimeRecorder = Some(apiCallRecorder.instance),
                     retryInterval = retryInterval,
                     maxRetries = maxRetries,
                     requestTimeoutOverride = requestTimeoutOverride
    )
    with GitLabClient[F] {

  def send[ResultType](method: Method, path: Uri, endpointName: String Refined NonEmpty)(
      mapResponse:             ResponseMapping[ResultType]
  )(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- request(method, uri, maybeAccessToken, endpointName)
    result  <- super.send(request)(mapResponse)
  } yield result

  private def request(method:           Method,
                      uri:              Uri,
                      maybeAccessToken: Option[AccessToken],
                      endpointName:     String Refined NonEmpty
  ): F[NamedRequest[F]] = HttpRequest(
    super.request(method, uri, maybeAccessToken),
    endpointName
  ).pure[F]

}

object GitLabClient {
  def apply[F[_]: Async: Logger](gitLabApiUrl: GitLabApiUrl,
                                 apiCallRecorder: GitLabApiCallRecorder[F],
                                 gitLabThrottler: Throttler[F, GitLab],
                                 retryInterval:   FiniteDuration = RestClient.SleepAfterConnectionIssue,
                                 maxRetries:      Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
                                 requestTimeoutOverride: Option[Duration] = None
  ): F[GitLabClientImpl[F]] = new GitLabClientImpl[F](gitLabApiUrl,
                                                      apiCallRecorder,
                                                      gitLabThrottler,
                                                      retryInterval,
                                                      maxRetries,
                                                      requestTimeoutOverride
  ).pure[F]

}
