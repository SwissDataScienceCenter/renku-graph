/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.missedevents

import cats.MonadError
import cats.effect.{Concurrent, Timer}
import cats.implicits._
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.graph.gitlab.GitLabRateLimitProvider
import eu.timepit.refined.auto._

import scala.language.higherKinds

sealed trait EventsSynchronization

object EventsSynchronizationThrottler {

  def apply[Interpretation[_]](
      gitLabRateLimitProvider: GitLabRateLimitProvider[Interpretation],
      throttler: (RateLimit,
                  Concurrent[Interpretation],
                  Timer[Interpretation]) => Interpretation[Throttler[Interpretation, EventsSynchronization]] =
        (rateLimit: RateLimit, concurrent: Concurrent[Interpretation], timer: Timer[Interpretation]) =>
          Throttler.apply[Interpretation, EventsSynchronization](rateLimit)(concurrent, timer)
  )(
      implicit ME: MonadError[Interpretation, Throwable],
      concurrent:  Concurrent[Interpretation],
      timer:       Timer[Interpretation]
  ): Interpretation[Throttler[Interpretation, EventsSynchronization]] =
    for {
      rateLimit        <- gitLabRateLimitProvider.get
      reducedRateLimit <- ME.fromEither(rateLimit / 10)
      throttler        <- throttler(reducedRateLimit, concurrent, timer)
    } yield throttler
}
