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
import eu.timepit.refined.api._
import eu.timepit.refined.predicates.all.Positive
import io.renku.control.{RateLimit, RateLimitUnit, Throttler}

trait GitLabThrottle[F[_]] extends Throttler[F, GitLabThrottle.GitLab]

object GitLabThrottle {
  sealed trait GitLab

  private def wrap[F[_]](t: Throttler[F, GitLab]): GitLabThrottle[F] =
    new GitLabThrottle[F] {
      override def throttle[O](value: F[O]) = t.throttle(value)
    }

  def none[F[_]: MonadThrow]: GitLabThrottle[F] =
    wrap(Throttler.noThrottling[F])

  def apply[F[_]: Async: Concurrent: Clock](
      items: Long Refined Positive,
      per:   RateLimitUnit
  ): F[GitLabThrottle[F]] =
    Throttler(RateLimit[GitLab](items, per)).map(wrap)
}
