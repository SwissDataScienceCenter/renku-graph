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

package ch.datascience.graph.gitlab

import cats.MonadError
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.generators.CommonGraphGenerators.rateLimits
import ch.datascience.generators.Generators.exceptions
import org.scalatest.WordSpec
import org.scalatest.Matchers._
import ch.datascience.generators.Generators.Implicits._
import org.scalamock.scalatest.MockFactory

import scala.concurrent.ExecutionContext

class GitLabThrottlerSpec extends WordSpec with MockFactory {

  "apply" should {

    "read the GitLab rate limit, divide it by 2 and instantiate the throttle with it" in new TestCase {
      val rateLimit = rateLimits.generateOne
      (gitLabRateLimitProvider.get _)
        .expects()
        .returning(context.pure(rateLimit))

      val throttler = mock[Throttler[IO, GitLab]]
      newThrottler
        .expects(rateLimit, concurrent, timer)
        .returning(IO.pure(throttler))

      GitLabThrottler(gitLabRateLimitProvider, newThrottler).unsafeRunSync()
    }

    "fail if reading the GitLab rate limit fails" in new TestCase {
      val exception = exceptions.generateOne
      (gitLabRateLimitProvider.get _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        GitLabThrottler(gitLabRateLimitProvider, newThrottler).unsafeRunSync()
      } shouldBe exception
    }
  }

  private implicit val contextShift: ContextShift[IO]          = IO.contextShift(ExecutionContext.global)
  private implicit val concurrent:   Concurrent[IO]            = IO.ioConcurrentEffect
  private implicit val timer:        Timer[IO]                 = IO.timer(ExecutionContext.global)
  private implicit val context:      MonadError[IO, Throwable] = MonadError[IO, Throwable]

  private trait TestCase {
    val gitLabRateLimitProvider = mock[IOGitLabRateLimitProvider]
    val newThrottler            = mockFunction[RateLimit, Concurrent[IO], Timer[IO], IO[Throttler[IO, GitLab]]]
  }
}
