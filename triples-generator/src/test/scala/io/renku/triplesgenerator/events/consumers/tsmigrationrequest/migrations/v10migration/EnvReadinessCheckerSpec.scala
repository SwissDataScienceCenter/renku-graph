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

package io.renku.triplesgenerator.events.consumers
package tsmigrationrequest.migrations.v10migration

import cats.effect.{Deferred, IO, Temporal}
import cats.syntax.all._
import io.renku.eventlog.api.EventLogClient
import io.renku.eventlog.api.EventLogClient.Result
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{ints, positiveInts}
import io.renku.graph.model.eventlogapi.ServiceStatus
import io.renku.graph.model.eventlogapi.ServiceStatus.{Capacity, SubscriptionStatus}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class EnvReadinessCheckerSpec extends AnyWordSpec with should.Matchers with MockFactory with IOSpec {

  "envReadyToTakeEvent" should {

    "succeed when three subsequent calls to EL's GET /status shows " +
      "there's some free capacity to process an AWAITING_GENERATION event" in new TestCase {

        givenELStatusReturning(
          nonZeroFreeCapacity.generateOne,
          0,
          nonZeroFreeCapacity.generateOne,
          0,
          0,
          nonZeroFreeCapacity.generateOne,
          nonZeroFreeCapacity.generateOne,
          nonZeroFreeCapacity.generateOne
        )

        val actual = Deferred.unsafe[IO, Unit]

        val call = checker.envReadyToTakeEvent >> actual.complete(())

        val (elapsed, _) = Temporal[IO].timed((call -> actual.get).parTupled).unsafeRunSync()

        val numberOfTriesBeforeSuccess = 7
        elapsed.toMillis should be > retryTimeout.toMillis * numberOfTriesBeforeSuccess
      }
  }

  private trait TestCase {
    val retryTimeout          = 500 millis
    private val totalCapacity = ints(min = 10, max = 20).generateOne
    def nonZeroFreeCapacity   = positiveInts(max = totalCapacity).map(_.value)
    private val elClient      = mock[EventLogClient[IO]]
    val checker               = new EnvReadinessCheckerImpl[IO](elClient, retryTimeout)

    def givenELStatusReturning(capacities: Int*): Unit =
      capacities foreach { capacity =>
        (() => elClient.getStatus)
          .expects()
          .returning(
            Result
              .success(
                ServiceStatus(
                  Set(
                    SubscriptionStatus(awaitinggeneration.categoryName, Capacity(totalCapacity, capacity).some)
                  )
                )
              )
              .pure[IO]
          )
      }
  }
}
