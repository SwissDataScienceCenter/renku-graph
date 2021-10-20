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

package io.renku.tokenrepository

import cats.effect._
import cats.effect.unsafe.implicits.global
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.testtools.MockedRunnableCollaborators
import io.renku.tokenrepository.repository.init.DbInitializer
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with MockedRunnableCollaborators
    with MockFactory
    with should.Matchers {

  "run" should {

    "return Success Exit Code if Sentry and the db initialize fine and http server starts up" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
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
      given(sentryInitializer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if db creation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(dbInitializer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting the http server fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val certificateLoader = mock[CertificateLoader[IO]]
    val sentryInitializer = mock[SentryInitializer[IO]]
    val dbInitializer     = mock[DbInitializer[IO]]
    val httpServer        = mock[HttpServer[IO]]
    val runner            = new MicroserviceRunner(certificateLoader, sentryInitializer, dbInitializer, httpServer)
  }
}
