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

package io.renku.microservices

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.renku.http.client.ServiceHealthChecker
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait ServiceReadinessChecker[F[_]] {
  def waitIfNotUp: F[Unit]
}

object ServiceReadinessChecker {
  def apply[F[_]: Async: Logger](microservicePort: Int Refined Positive): F[ServiceReadinessChecker[F]] = for {
    urlFinder     <- MicroserviceUrlFinder[F](microservicePort)
    healthChecker <- ServiceHealthChecker[F]
  } yield new ServiceReadinessCheckerImpl(urlFinder, healthChecker)
}

class ServiceReadinessCheckerImpl[F[_]: Async: Logger](
    urlFinder:     MicroserviceUrlFinder[F],
    healthChecker: ServiceHealthChecker[F]
) extends ServiceReadinessChecker[F] {

  import healthChecker._

  override def waitIfNotUp: F[Unit] = urlFinder.findBaseUrl() >>= checkAndWait

  private def checkAndWait(url: MicroserviceBaseUrl): F[Unit] =
    ping(url) >>= {
      case true  => ().pure[F]
      case false => Temporal[F].delayBy(Logger[F].info("Service not ready") >> checkAndWait(url), 1 second)
    }
}
