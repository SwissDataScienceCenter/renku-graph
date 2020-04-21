/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.eventlog.rescheduling

import cats.effect.IO
import cats.implicits._
import ch.datascience.controllers.InfoMessage
import ch.datascience.controllers.InfoMessage._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ReSchedulingEndpointSpec extends WordSpec with MockFactory {

  "triggerReScheduling" should {

    s"trigger events re-scheduling and return $Accepted" in new TestCase {

      (reScheduler.scheduleEventsForProcessing _)
        .expects()
        .returning(IO.unit)

      val request = Request(Method.POST, uri"events/status/NEW")

      val response = triggerReScheduling().unsafeRunSync()

      response.status                        shouldBe Accepted
      response.contentType                   shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync shouldBe InfoMessage("Events re-scheduling triggered")

      logger.expectNoLogs()
    }

    s"return $Accepted regardless of the outcome of the call to events re-scheduling" in new TestCase {

      val exception = exceptions.generateOne
      (reScheduler.scheduleEventsForProcessing _)
        .expects()
        .returning(exception.raiseError[IO, Unit])

      val request = Request(Method.POST, uri"events/status/NEW")

      val response = triggerReScheduling().unsafeRunSync()

      response.status                        shouldBe Accepted
      response.contentType                   shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[InfoMessage].unsafeRunSync shouldBe InfoMessage("Events re-scheduling triggered")

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    val reScheduler         = mock[IOReScheduler]
    val logger              = TestLogger[IO]()
    val triggerReScheduling = new ReSchedulingEndpointImpl(reScheduler, logger).triggerReScheduling _
  }
}
