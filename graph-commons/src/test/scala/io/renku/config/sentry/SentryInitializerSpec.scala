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

package io.renku.config.sentry

import cats.MonadThrow
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.sentry.SentryOptions
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class SentryInitializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    "initialise Sentry with the url built from the given config if config given" in new TestCase {

      val optionsCapture = CaptureOne[SentryOptions]()
      initSentry.expects(capture(optionsCapture)).returning(())

      val sentryConfig = sentryConfigs.generateOne

      sentryInitializer(sentryConfig.some).run shouldBe ().pure[Try]

      val options = optionsCapture.value
      options.getDsn         shouldBe sentryConfig.baseUrl.value
      options.getServerName  shouldBe sentryConfig.serviceName.value
      options.getEnvironment shouldBe sentryConfig.environmentName.value
      options.getRelease     shouldBe show"renku-graph@${sentryConfig.serviceVersion}"
    }

    "do nothing if no config given" in new TestCase {
      val maybeConfig = None
      sentryInitializer(maybeConfig).run shouldBe MonadThrow[Try].unit
    }

    "fail if Sentry initialisation fails" in new TestCase {
      val sentryConfig = sentryConfigs.generateOne

      val exception = exceptions.generateOne
      initSentry.expects(*).throwing(exception)

      sentryInitializer(sentryConfig.some).run shouldBe exception.raiseError[Try, Unit]
    }
  }

  private trait TestCase {
    val initSentry        = mockFunction[SentryOptions, Unit]
    val sentryInitializer = new SentryInitializerImpl[Try](_, initSentry)
  }
}
