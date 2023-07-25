/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
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
import io.renku.http.rest.paging.model.{Page, Total}
import io.renku.metrics.{GitLabApiCallRecorder, MetricsRegistry}
import org.http4s.Method.{DELETE, GET, HEAD, POST, PUT}
import org.http4s.circe.{jsonEncoder, jsonEncoderOf}
import org.http4s.{EntityEncoder, Method, Response, Uri, UrlForm}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

trait GitLabClient[F[_]] {

  def get[ResultType](path: Uri, endpointName: String Refined NonEmpty)(
      mapResponse: ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType]

  def head[ResultType](path: Uri, endpointName: String Refined NonEmpty)(
      mapResponse: ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType]

  def post[ResultType](path: Uri, endpointName: String Refined NonEmpty, payload: Json)(
      mapResponse: ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType]

  def put[ResultType](path: Uri, endpointName: String Refined NonEmpty, payload: UrlForm)(
      mapResponse: ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType]

  def delete[ResultType](path: Uri, endpointName: String Refined NonEmpty)(
      mapResponse: ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType]
}

final class GitLabClientImpl[F[_]: Async: Logger](
    gitLabApiUrl:           GitLabApiUrl,
    apiCallRecorder:        GitLabApiCallRecorder[F],
    gitLabThrottler:        Throttler[F, GitLab],
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(
      gitLabThrottler,
      maybeTimeRecorder = Some(apiCallRecorder.instance),
      retryInterval = retryInterval,
      maxRetries = maxRetries,
      requestTimeoutOverride = requestTimeoutOverride
    )
    with GitLabClient[F] {

  override def get[ResultType](path: Uri, endpointName: String Refined NonEmpty)(
      mapResponse: ResponseMapping[ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- secureNamedRequest(GET, uri, endpointName)
    result  <- super.send(request)(mapResponse)
  } yield result

  override def head[ResultType](path: Uri, endpointName: String Refined NonEmpty)(
      mapResponse: ResponseMapping[ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- secureNamedRequest(HEAD, uri, endpointName)
    result  <- super.send(request)(mapResponse)
  } yield result

  override def post[ResultType](path: Uri, endpointName: String Refined NonEmpty, payload: Json)(
      mapResponse: ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- secureNamedRequest(POST, uri, endpointName, payload)
    result  <- super.send(request)(mapResponse)
  } yield result

  override def put[ResultType](path: Uri, endpointName: String Refined NonEmpty, payload: UrlForm)(
      mapResponse: ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- secureNamedRequest(PUT, uri, endpointName, payload)
    result  <- super.send(request)(mapResponse)
  } yield result

  override def delete[ResultType](path: Uri, endpointName: Refined[String, NonEmpty])(
      mapResponse: ResponseMappingF[F, ResultType]
  )(implicit maybeAccessToken: Option[AccessToken]): F[ResultType] = for {
    uri     <- validateUri(show"$gitLabApiUrl/$path")
    request <- secureNamedRequest(DELETE, uri, endpointName)
    result  <- super.send(request)(mapResponse)
  } yield result

  protected implicit val jsonEntityEncoder: EntityEncoder[IO, Json] = jsonEncoderOf[IO, Json]

  private def secureNamedRequest(method: Method, uri: Uri, endpointName: String Refined NonEmpty)(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[NamedRequest[F]] = HttpRequest(
    super.secureRequest(method, uri),
    endpointName
  ).pure[F]

  private def secureNamedRequest[T](method: Method, uri: Uri, endpointName: String Refined NonEmpty, payload: T)(
      implicit
      maybeAccessToken: Option[AccessToken],
      eEnc:             EntityEncoder[F, T]
  ): F[NamedRequest[F]] =
    secureNamedRequest(method, uri, endpointName)
      .map(originalRequest => originalRequest.copy(request = originalRequest.request.withEntity(payload)))
}

object GitLabClient {

  def apply[F[_]](implicit ev: GitLabClient[F]): GitLabClient[F] = ev

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

  def maybeNextPage[F[_]: MonadThrow](response: Response[F]): F[Option[Page]] =
    response.headers
      .get(ci"X-Next-Page")
      .flatMap(_.head.value.toIntOption)
      .map(Page.from)
      .map(MonadThrow[F].fromEither(_))
      .sequence

  def maybeTotalPages[F[_]: MonadThrow](response: Response[F]): F[Option[Total]] = {
    val header = ci"X-Total-Pages"
    response.headers
      .get(header)
      .flatMap(_.head.value.toIntOption)
      .map(Total.from)
      .map(MonadThrow[F].fromEither(_))
      .sequence
  }
}
