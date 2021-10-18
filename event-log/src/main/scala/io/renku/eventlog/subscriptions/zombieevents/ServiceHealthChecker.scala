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

package io.renku.eventlog.subscriptions.zombieevents

import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.control.Throttler
import io.renku.http.client.RestClient
import io.renku.http.client.RestClient.MaxRetriesAfterConnectionTimeout
import io.renku.microservices.MicroserviceBaseUrl
import org.http4s.Status.Ok
import org.http4s._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NonFatal

private trait ServiceHealthChecker[Interpretation[_]] {
  def ping(microserviceBaseUrl: MicroserviceBaseUrl): Interpretation[Boolean]
}

private object ServiceHealthChecker {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      concurrentEffect: ConcurrentEffect[IO],
      timer:            Timer[IO]
  ): IO[ServiceHealthChecker[IO]] = IO {
    new ServiceHealthCheckerImpl(logger)
  }
}

private class ServiceHealthCheckerImpl[Interpretation[_]: ConcurrentEffect: Timer](
    logger:                  Logger[Interpretation],
    retryInterval:           FiniteDuration = 1 second,
    maxRetries:              Int Refined NonNegative = MaxRetriesAfterConnectionTimeout
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, ServiceHealthChecker[Interpretation]](Throttler.noThrottling,
                                                                             logger,
                                                                             retryInterval = retryInterval,
                                                                             maxRetries = maxRetries
    )
    with ServiceHealthChecker[Interpretation] {

  override def ping(microserviceBaseUrl: MicroserviceBaseUrl): Interpretation[Boolean] = {
    for {
      uri    <- validateUri((microserviceBaseUrl / "ping").toString)
      result <- send(request(Method.GET, uri))(mapResponse)
    } yield result
  } recover { case NonFatal(_) => false }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Boolean]] = {
    case (Ok, _, _) => true.pure[Interpretation]
    case (_, _, _)  => false.pure[Interpretation]
  }
}
