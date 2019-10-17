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

package ch.datascience.config.sentry

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.client.UrlEncoder._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class SentryInitializerSpec extends WordSpec with MockFactory {

  "run" should {

    "initialise Sentry with the url built from the given config if config given" in new TestCase {
      val sentryConfig = sentryConfigs.generateOne

      initSentry.expects {
        s"${sentryConfig.baseUrl}?" +
          s"stacktrace.app.packages=&" +
          s"servername=${urlEncode(sentryConfig.serviceName.toString)}&" +
          s"environment=${urlEncode(sentryConfig.environmentName.toString)}"
      }

      sentryInitializer(Some(sentryConfig)).run shouldBe context.unit
    }

    "do nothing if no config given" in new TestCase {
      val maybeConfig = None
      sentryInitializer(maybeConfig).run shouldBe context.unit
    }

    "fail if Sentry initialisation fails" in new TestCase {
      val sentryConfig = sentryConfigs.generateOne

      val exception = exceptions.generateOne
      initSentry.expects(*).throwing(exception)

      sentryInitializer(Some(sentryConfig)).run shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val initSentry        = mockFunction[String, Unit]
    val sentryInitializer = new SentryInitializer[Try](_, initSentry)
  }
}
