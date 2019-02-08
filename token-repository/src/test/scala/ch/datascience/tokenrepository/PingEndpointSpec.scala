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

package ch.datascience.tokenrepository

import cats.effect.IO
import ch.datascience.http.EndpointTester._
import org.http4s.dsl.io._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class PingEndpointSpec extends WordSpec {

  "ping" should {

    "respond with OK and 'pong' body" in new TestCase {
      val response = endpoint.call(
        Request(Method.GET, Uri.uri("/ping"))
      )

      response.status       shouldBe Status.Ok
      response.body[String] shouldBe "pong"
    }
  }

  private trait TestCase {
    val endpoint = new PingEndpoint[IO].ping.orNotFound
  }
}
