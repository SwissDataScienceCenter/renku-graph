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

package io.renku.eventlog.status

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.renku.eventlog.events.producers.{EventProducerStatus, EventProducersRegistry}
import io.renku.eventlog.events.producers.Generators.eventProducerStatuses
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.eventlogapi
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.http4s.headers.`Content-Type`
import org.http4s.MediaType._
import org.http4s.circe.CirceEntityDecoder._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class StatusEndpointSpec extends AnyWordSpec with should.Matchers with MockFactory with IOSpec {

  "`GET /status`" should {

    "collect event producers statuses and return them as JSON" in new TestCase {

      val statuses = eventProducerStatuses.generateSet()
      (() => eventProducersRegistry.getStatus).expects().returning(statuses.pure[IO])

      val response = endpoint.`GET /status`.unsafeRunSync()

      response.status                                        shouldBe Status.Ok
      response.contentType                                   shouldBe `Content-Type`(application.json).some
      response.as[eventlogapi.ServiceStatus].unsafeRunSync() shouldBe toApiStatus(statuses)
    }

    "fail if finding event producers statuses fails" in new TestCase {

      val exception = exceptions.generateOne
      (() => eventProducersRegistry.getStatus).expects().returning(exception.raiseError[IO, Set[EventProducerStatus]])

      val response = endpoint.`GET /status`.unsafeRunSync()

      response.status                          shouldBe Status.InternalServerError
      response.contentType                     shouldBe `Content-Type`(application.json).some
      response.as[Json].unsafeRunSync().noSpaces should include("Finding EL status failed")
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventProducersRegistry = mock[EventProducersRegistry[IO]]
    val endpoint               = new StatusEndpointImpl[IO](eventProducersRegistry)
  }

  private lazy val toApiStatus: Set[EventProducerStatus] => eventlogapi.ServiceStatus = statuses =>
    eventlogapi.ServiceStatus(
      statuses.map(toApiSubscriptionStatus)
    )

  private lazy val toApiSubscriptionStatus: EventProducerStatus => eventlogapi.ServiceStatus.SubscriptionStatus =
    status =>
      eventlogapi.ServiceStatus.SubscriptionStatus(
        status.categoryName,
        status.maybeCapacity.map(toApiCapacity)
      )

  private lazy val toApiCapacity: EventProducerStatus.Capacity => eventlogapi.ServiceStatus.Capacity =
    capacity => eventlogapi.ServiceStatus.Capacity(capacity.total.value, capacity.free.value)
}
