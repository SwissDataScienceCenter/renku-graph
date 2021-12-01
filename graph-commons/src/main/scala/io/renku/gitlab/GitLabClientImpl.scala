package io.renku.gitlab

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.NonNegative
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.http.client.HttpRequest.NamedRequest
import io.renku.http.client.{AccessToken, HttpRequest, RestClient}
import org.http4s.{Method, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

final class GitLabClientImpl[F[_]: Async: Logger](
    apiCallRecorder:        ApiCallRecorder[F],
    gitLabThrottler:        Throttler[F, GitLab],
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(gitLabThrottler,
                     maybeTimeRecorder = Some(apiCallRecorder.instance),
                     retryInterval = retryInterval,
                     maxRetries = maxRetries,
                     requestTimeoutOverride = requestTimeoutOverride
    ) {

  def send[ResultType](method:           Method,
                       urlString:        String,
                       maybeAccessToken: Option[AccessToken],
                       endpointName:     String Refined NonEmpty,
                       queryParams:      (String, String)*
  )(mapResponse:                         ResponseMapping[ResultType]): F[ResultType] = for {
    uri     <- validateUri(urlString).map(uri => uri.withQueryParams(queryParams.toMap))
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

//  override lazy val validateUri: String => F[Uri] = super.validateUri

}
