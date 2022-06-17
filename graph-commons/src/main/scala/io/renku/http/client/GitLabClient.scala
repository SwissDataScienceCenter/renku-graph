/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.http.client

import cats.effect.{Async, IO}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.NonNegative
import io.circe.Json
import io.renku.config.GitLab
import io.renku.control.{RateLimit, Throttler}
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabApiUrl
import io.renku.http.client.HttpRequest.NamedRequest
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.metrics.{GitLabApiCallRecorder, MetricsRegistry}
import org.http4s.Method.{DELETE, GET, POST}
import org.http4s.circe.{jsonEncoder, jsonEncoderOf}
import org.http4s.{EntityEncoder, Method, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

trait GitLabClient[F[_]] {

  def get[ResultType](path:    Uri, endpointName: String Refined NonEmpty)(
      mapResponse:             ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType]

  def post[ResultType](path:   Uri, endpointName: String Refined NonEmpty, payload: Json)(
      mapResponse:             ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType]

  def delete[ResultType](path: Uri, endpointName: String Refined NonEmpty)(
      mapResponse:             ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType]

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

  def get[ResultType](path:    Uri, endpointName: String Refined NonEmpty)(
      mapResponse:             ResponseMapping[ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- secureNamedRequest(GET, uri, endpointName)
    result  <- super.send(request)(mapResponse)
  } yield result

  def post[ResultType](path:   Uri, endpointName: String Refined NonEmpty, payload: Json)(
      mapResponse:             ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- secureNamedRequest(uri, endpointName, payload)
    result  <- super.send(request)(mapResponse)
  } yield result

  override def delete[ResultType](path: Uri, endpointName: Refined[String, NonEmpty])(
      mapResponse:                      ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken:          Option[AccessToken]): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- secureNamedRequest(DELETE, uri, endpointName)
    result  <- super.send(request)(mapResponse)
  } yield result

  protected implicit val jsonEntityEncoder: EntityEncoder[IO, Json] = jsonEncoderOf[IO, Json]

  private def secureNamedRequest(method: Method, uri: Uri, endpointName: String Refined NonEmpty)(implicit
      maybeAccessToken:                  Option[AccessToken]
  ): F[NamedRequest[F]] = HttpRequest(
    super.secureRequest(method, uri),
    endpointName
  ).pure[F]

  private def secureNamedRequest(uri: Uri, endpointName: String Refined NonEmpty, payload: Json)(implicit
      maybeAccessToken:               Option[AccessToken]
  ): F[NamedRequest[F]] =
    secureNamedRequest(POST, uri, endpointName)
      .map(originalRequest => originalRequest.copy(request = originalRequest.request.withEntity(payload)))
}

object GitLabClient {
  def apply[F[_]: Async: Logger: MetricsRegistry](
      retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
      maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
      requestTimeoutOverride: Option[Duration] = None
  ): F[GitLabClientImpl[F]] = for {
    gitLabRateLimit <- RateLimit.fromConfig[F, GitLab]("services.gitlab.rate-limit")
    gitLabThrottler <- Throttler[F, GitLab](gitLabRateLimit)
    gitLabUrl       <- GitLabUrlLoader[F]()
    apiCallRecorder <- GitLabApiCallRecorder[F]
  } yield new GitLabClientImpl[F](gitLabUrl.apiV4,
                                  apiCallRecorder,
                                  gitLabThrottler,
                                  retryInterval,
                                  maxRetries,
                                  requestTimeoutOverride
  )
}
