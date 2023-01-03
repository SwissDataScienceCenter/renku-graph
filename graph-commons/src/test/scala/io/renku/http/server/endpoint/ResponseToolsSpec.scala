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

package io.renku.http.server.endpoint

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.testtools.IOSpec
import org.http4s.MediaRange.`*/*`
import org.http4s.MediaType.application
import org.http4s.headers.Accept
import org.http4s.{Headers, Request, Response, Status}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ResponseToolsSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "whenAccept" should {

    "return the result for matched MediaType" in new TestCase {

      val body = nonEmptyStrings().generateOne
      bodyFactory.expects().returns(body)

      implicit val request: Request[IO] = Request(headers = Headers(Accept(application.json)))

      val response = whenAccept(
        application.json      --> Response[IO](Status.Ok).withEntity(bodyFactory()).pure[IO],
        application.`ld+json` --> Response[IO](Status.Ok).withEntity(bodyFactory()).pure[IO]
      )(default = Response[IO](Status.Accepted).pure[IO])

      response.map(_.status).unsafeRunSync()         shouldBe Status.Ok
      response.flatMap(_.as[String]).unsafeRunSync() shouldBe body
    }

    "return the default result for 'Accept: */*'" in new TestCase {
      implicit val request: Request[IO] = Request(headers = Headers(Accept(`*/*`)))

      val response = whenAccept {
        application.json --> Response[IO](Status.Ok).withEntity(bodyFactory()).pure[IO]
      }(default = Response[IO](Status.Accepted).pure[IO])

      response.map(_.status).unsafeRunSync()         shouldBe Status.Accepted
      response.flatMap(_.as[String]).unsafeRunSync() shouldBe ""
    }

    "return the default result when no Accept header" in new TestCase {
      implicit val request: Request[IO] = Request()

      val response = whenAccept {
        application.json --> Response[IO](Status.Ok).withEntity(bodyFactory()).pure[IO]
      }(default = Response[IO](Status.Accepted).pure[IO])

      response.map(_.status).unsafeRunSync()         shouldBe Status.Accepted
      response.flatMap(_.as[String]).unsafeRunSync() shouldBe ""
    }

    "return BadRequest with an error message result when no matching MediaType defined" in new TestCase {
      val otherAccept = Accept(application.`ld+json`)
      implicit val request: Request[IO] = Request(headers = Headers(otherAccept))

      val response = whenAccept {
        application.json --> Response[IO](Status.Ok).withEntity(bodyFactory()).pure[IO]
      }(default = Response[IO](Status.Accepted).pure[IO])

      response.map(_.status).unsafeRunSync() shouldBe Status.BadRequest
      response
        .flatMap(_.as[String])
        .unsafeRunSync() shouldBe s"Accept: ${otherAccept.values.map(_.mediaRange.toString()).intercalate(", ")} not supported"
    }
  }

  private trait TestCase {
    val bodyFactory = mockFunction[String]
  }
}
