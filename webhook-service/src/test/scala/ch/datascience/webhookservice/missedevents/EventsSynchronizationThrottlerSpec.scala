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
import cats.effect._
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.config.GitLab
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext

class EventsSynchronizationThrottlerSpec extends WordSpec with MockFactory {

  "apply" should {

    "divide the given GitLab RateLimit by 10 and instantiate the throttler with it" in new TestCase {
      val gitLabRateLimit = (rateLimits[GitLab] retryUntil (limit => (limit / 10).isRight)).generateOne

      val throttler                      = mock[Throttler[IO, EventsSynchronization]]
      val eventsSynchronizationRateLimit = gitLabRateLimit./[EventsSynchronization](10).fold(throw _, identity)
      newThrottler
        .expects(eventsSynchronizationRateLimit, concurrent, timer)
        .returning(IO.pure(throttler))

      EventsSynchronizationThrottler(gitLabRateLimit, newThrottler).unsafeRunSync()
    }
  }

  private implicit val contextShift: ContextShift[IO]          = IO.contextShift(ExecutionContext.global)
  private implicit val concurrent:   Concurrent[IO]            = IO.ioConcurrentEffect
  private implicit val timer:        Timer[IO]                 = IO.timer(ExecutionContext.global)
  private implicit val context:      MonadError[IO, Throwable] = MonadError[IO, Throwable]

  private trait TestCase {
    val newThrottler = mockFunction[RateLimit[EventsSynchronization],
                                    Concurrent[IO],
                                    Timer[IO],
                                    IO[Throttler[IO, EventsSynchronization]]]
  }
}
