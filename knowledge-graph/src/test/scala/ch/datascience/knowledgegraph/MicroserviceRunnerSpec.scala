/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph

import cats.MonadError
import cats.effect.{ContextShift, ExitCode, IO}
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.IOSentryInitializer
import ch.datascience.knowledgegraph.metrics.KGMetrics
import ch.datascience.testtools.MockedRunnableCollaborators
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with MockedRunnableCollaborators
    with MockFactory
    with should.Matchers {

  "run" should {

    "return Success Exit Code if Sentry initializes and http server starts up" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(kgMetrics).succeeds(returning = ())
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      given(certificateLoader).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if Sentry initialization fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      val exception = exceptions.generateOne
      (sentryInitializer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting the http server fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(kgMetrics).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "return Success ExitCode even if Knowledge Graph Metrics initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(kgMetrics).fails(becauseOf = exception)
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val certificateLoader = mock[CertificateLoader[IO]]
    val sentryInitializer = mock[IOSentryInitializer]
    val httpServer        = mock[IOHttpServer]
    val kgMetrics         = mock[KGMetrics[IO]]
    val runner            = new MicroserviceRunner(certificateLoader, sentryInitializer, httpServer, kgMetrics)
  }
}
