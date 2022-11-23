/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.eventpayload

import cats.effect.IO
import cats.syntax.all._
import io.renku.eventlog.eventpayload.EventPayloadFinder.PayloadData
import io.renku.graph.model.{EventsGenerators, GraphModelGenerators}
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.generators.Generators.Implicits._
import org.http4s.{MediaType, Status}
import org.http4s.headers.{`Content-Length`, `Content-Type`}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger
import scodec.bits.ByteVector

class EventPayloadEndpointSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "getEventPayload" should {
    "return OK with bytes if event and payload is found" in new TestCase {
      val eventId     = EventsGenerators.eventIds.generateOne
      val projectPath = GraphModelGenerators.projectPaths.generateOne
      val someData    = PayloadData(ByteVector(0, 10, -10, 5))

      (finder.findEventPayload _)
        .expects(eventId, projectPath)
        .returning(someData.some.pure[IO])

      val resp = endpoint.getEventPayload(eventId, projectPath).unsafeRunSync()
      resp.status                                   shouldBe Status.Ok
      resp.contentType                              shouldBe Some(`Content-Type`(MediaType.application.gzip))
      resp.headers.get[`Content-Length`].get.length shouldBe someData.length
      resp.as[ByteVector].unsafeRunSync()           shouldBe someData.data
    }

    "return NotFound if no payload is available" in new TestCase {
      val eventId     = EventsGenerators.eventIds.generateOne
      val projectPath = GraphModelGenerators.projectPaths.generateOne

      (finder.findEventPayload _)
        .expects(eventId, projectPath)
        .returning(None.pure[IO])

      val resp = endpoint.getEventPayload(eventId, projectPath).unsafeRunSync()
      resp.status      shouldBe Status.NotFound
      resp.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
    }
  }

  class TestCase {
    implicit val logger: Logger[IO] = TestLogger()

    val finder   = mock[EventPayloadFinder[IO]]
    val endpoint = EventPayloadEndpoint[IO](finder)
  }
}
