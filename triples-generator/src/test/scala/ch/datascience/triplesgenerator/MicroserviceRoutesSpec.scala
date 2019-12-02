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

package ch.datascience.triplesgenerator

import cats.effect.IO
import ch.datascience.http.server.EndpointTester._
import org.http4s.Status._
import org.http4s._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec extends WordSpec with MockFactory {

  "routes" should {

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {

      val response = routes.test(Request(Method.GET, uri"ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  private trait TestCase {
    val routes = new MicroserviceRoutes[IO]().routes.or(notAvailableResponse)
  }
}
