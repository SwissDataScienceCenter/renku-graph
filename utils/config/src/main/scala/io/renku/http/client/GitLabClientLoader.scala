/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.{Async, Clock, Concurrent}
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader
import io.renku.config.RenkuConfigReader._
import io.renku.control.RateLimitLoader
import io.renku.logging.ExecutionTimeRecorderLoader
import io.renku.metrics.{Histogram, MetricsRegistry}
import org.typelevel.log4cats.Logger

object GitLabClientLoader {
  private val rateLimitKey = "services.gitlab.rate-limit"
  private val gitlabUrlKey = "services.gitlab.url"

  def gitLabUrl[F[_]: MonadThrow](config: Config) =
    ConfigLoader.find[F, GitLabUrl](gitlabUrlKey, config)

  def apply[F[_]: Async: Concurrent: Clock: Logger: MetricsRegistry](
      config: Config = ConfigFactory.load()
  ): F[GitLabClient[F]] =
    for {
      gitLabUrl <- gitLabUrl(config)
      rateLimit <- RateLimitLoader.fromConfig[F, Any](rateLimitKey, config)
      throttler <- GitLabThrottle[F](rateLimit.items, rateLimit.per)
      client <- GitLabClient.create[F](
                  throttler,
                  gitLabUrl,
                  (hg: Histogram[F]) => ExecutionTimeRecorderLoader(config, maybeHistogram = Some(hg))
                )
    } yield client
}
