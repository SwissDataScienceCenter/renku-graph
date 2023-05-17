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

package io.renku.http.client

import cats.effect.IO
import com.comcast.ip4s._
import eu.timepit.refined.auto._
import io.renku.control.Throttler
import io.renku.http.server.SimpleServer
import io.renku.testtools.IOSpec
import org.http4s.implicits._
import org.http4s.{Method, Status, Uri}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._

class ClientTimeoutTest extends AnyFlatSpec with should.Matchers with IOSpec {

  it should "be configurable for request timeouts" ignore {
    val client = new ClientTimeoutTest.MyClient(Some(90.seconds))

    SimpleServer
      .server(port"8088")
      .use { _ =>
        client.get(uri"http://localhost:8088").map(_ shouldBe Status.Ok)
      }
      .unsafeRunSync()
  }
}

object ClientTimeoutTest {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger

  final class MyClient(timeout: Option[Duration])
      extends RestClient[IO, Nothing](Throttler.noThrottling, None, 5.seconds, 5, timeout, timeout) {
    def get(uri: Uri): IO[Status] =
      send(request(Method.GET, uri)) { case r => IO.pure(r._1) }
  }
}
