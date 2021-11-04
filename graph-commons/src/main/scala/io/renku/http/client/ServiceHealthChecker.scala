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

package io.renku.http.client

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.control.Throttler
import io.renku.http.client.RestClient.MaxRetriesAfterConnectionTimeout
import io.renku.microservices.MicroserviceBaseUrl
import org.http4s.Status.Ok
import org.http4s._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.util.control.NonFatal

trait ServiceHealthChecker[F[_]] {
  def ping(microserviceBaseUrl: MicroserviceBaseUrl): F[Boolean]
}

object ServiceHealthChecker {
  def apply[F[_]: Async: Logger]: F[ServiceHealthChecker[F]] = MonadThrow[F].catchNonFatal {
    new ServiceHealthCheckerImpl()
  }
}

class ServiceHealthCheckerImpl[F[_]: Async: Logger](
    retryInterval: FiniteDuration = 1 second,
    maxRetries:    Int Refined NonNegative = MaxRetriesAfterConnectionTimeout
) extends RestClient(Throttler.noThrottling, retryInterval = retryInterval, maxRetries = maxRetries)
    with ServiceHealthChecker[F] {

  override def ping(microserviceBaseUrl: MicroserviceBaseUrl): F[Boolean] = {
    for {
      uri    <- validateUri((microserviceBaseUrl / "ping").toString)
      result <- send(request(Method.GET, uri))(mapResponse)
    } yield result
  } recover { case NonFatal(_) => false }

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Boolean]] = {
    case (Ok, _, _) => true.pure[F]
    case (_, _, _)  => false.pure[F]
  }
}
