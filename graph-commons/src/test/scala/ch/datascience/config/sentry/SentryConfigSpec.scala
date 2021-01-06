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

package ch.datascience.config.sentry

import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.config.sentry.SentryConfig.SentryStackTracePackage
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class SentryConfigSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "apply" should {

    "return None if 'services.sentry.enabled' is 'false'" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "sentry" -> Map(
              "enabled" -> "false"
            ).asJava
          ).asJava
        ).asJava
      )

      SentryConfig[Try](config) shouldBe Success(None)
    }

    "return a SentryConfig if 'services.sentry.enabled' is 'true' and all " +
      "'services.sentry.url', 'services.sentry.service-name', 'services.sentry.environment-name' and 'services.sentry.stacktrace-package' are set" in {
        forAll { sentryConfig: SentryConfig =>
          val config = ConfigFactory.parseMap(
            Map(
              "services" -> Map(
                "sentry" -> Map(
                  "enabled"            -> "true",
                  "url"                -> sentryConfig.baseUrl.value,
                  "service-name"       -> sentryConfig.serviceName.value,
                  "environment-name"   -> sentryConfig.environmentName.value,
                  "stacktrace-package" -> sentryConfig.stackTracePackage.value
                ).asJava
              ).asJava
            ).asJava
          )

          SentryConfig[Try](config) shouldBe Success(Some(sentryConfig))
        }
      }

    "return a SentryConfig if 'services.sentry.enabled' is 'true' but 'services.sentry.stacktrace-package' is invalid and replace it with an empty string" in {

      val sentryConfig = sentryConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "sentry" -> Map(
              "enabled"          -> "true",
              "url"              -> sentryConfig.baseUrl.value,
              "service-name"     -> sentryConfig.serviceName.value,
              "environment-name" -> sentryConfig.environmentName.value
            ).asJava
          ).asJava
        ).asJava
      )

      SentryConfig[Try](config) shouldBe Success(
        Some(sentryConfig.copy(stackTracePackage = SentryStackTracePackage.empty))
      )
    }

    "fail if 'services.sentry.enabled' is 'true' but 'services.sentry.url' is invalid" in {
      val sentryConfig = sentryConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "sentry" -> Map(
              "enabled"            -> "true",
              "url"                -> "",
              "service-name"       -> sentryConfig.serviceName.value,
              "environment-name"   -> sentryConfig.environmentName.value,
              "stacktrace-package" -> sentryConfig.stackTracePackage.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = SentryConfig[Try](config)

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if 'services.sentry.enabled' is 'true' but 'services.sentry.service-name' is invalid" in {
      val sentryConfig = sentryConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "sentry" -> Map(
              "enabled"            -> "true",
              "url"                -> sentryConfig.baseUrl.value,
              "service-name"       -> "",
              "environment-name"   -> sentryConfig.environmentName.value,
              "stacktrace-package" -> sentryConfig.stackTracePackage.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = SentryConfig[Try](config)

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if 'services.sentry.enabled' is 'true' but 'services.sentry.environment-name' is invalid" in {
      val sentryConfig = sentryConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "sentry" -> Map(
              "enabled"            -> "true",
              "url"                -> sentryConfig.baseUrl.value,
              "service-name"       -> sentryConfig.serviceName.value,
              "environment-name"   -> "",
              "stacktrace-package" -> sentryConfig.stackTracePackage.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = SentryConfig[Try](config)

      exception shouldBe a[ConfigLoadingException]
    }
  }
}
