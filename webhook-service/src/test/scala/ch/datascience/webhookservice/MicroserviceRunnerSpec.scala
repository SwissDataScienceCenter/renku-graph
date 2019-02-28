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

package ch.datascience.webhookservice

import cats.MonadError
import cats.effect._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators
import ch.datascience.http.server.PingEndpoint
import ch.datascience.webhookservice.eventprocessing.HookEventEndpoint
import ch.datascience.webhookservice.hookcreation.HookCreationEndpoint
import ch.datascience.webhookservice.hookvalidation.HookValidationEndpoint
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext

class MicroserviceRunnerSpec extends WordSpec with MockFactory {

  "run" should {

    "return Success Exit Code if http server start-up were successful" in new TestCase {

      (httpServer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      runner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if starting http server fails" in new TestCase {

      val exception = Generators.exceptions.generateOne
      (httpServer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val httpServer = mock[IOHttpServer]
    val runner     = new MicroserviceRunner(httpServer)
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private class IOHttpServer(
      pingEndpoint:           PingEndpoint[IO],
      hookEventEndpoint:      HookEventEndpoint[IO],
      hookCreationEndpoint:   HookCreationEndpoint[IO],
      hookValidationEndpoint: HookValidationEndpoint[IO]
  ) extends HttpServer[IO](pingEndpoint, hookEventEndpoint, hookCreationEndpoint, hookValidationEndpoint)
}
