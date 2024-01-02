/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

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
      val versionConfig = ConfigFactory.parseMap(
        Map("version" -> serviceVersions.generateOne.value).asJava
      )

      SentryConfig[Try](config, versionConfig.some) shouldBe None.pure[Try]
    }

    "return a SentryConfig if 'services.sentry.enabled' is 'true' and all " +
      "'services.sentry.url', 'services.sentry.environment', 'service-name' and 'version' are set" in {
        forAll { sentryConfig: SentryConfig =>
          val config = ConfigFactory.parseMap(
            Map[String, AnyRef](
              "service-name" -> sentryConfig.serviceName.value,
              "services" -> Map(
                "sentry" -> Map(
                  "enabled"     -> "true",
                  "dsn"         -> sentryConfig.baseUrl.value,
                  "environment" -> sentryConfig.environmentName.value
                ).asJava
              ).asJava
            ).asJava
          )

          val versionConfig = ConfigFactory.parseMap(
            Map("version" -> sentryConfig.serviceVersion.value).asJava
          )

          SentryConfig[Try](config, versionConfig.some) shouldBe sentryConfig.some.pure[Try]
        }
      }

    "fail if 'services.sentry.enabled' is 'true' but 'services.sentry.dsn' is invalid" in {
      val sentryConfig = sentryConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map[String, AnyRef](
          "service-name" -> sentryConfig.serviceName.value,
          "services" -> Map(
            "sentry" -> Map(
              "enabled"     -> "true",
              "dsn"         -> "",
              "environment" -> sentryConfig.environmentName.value
            ).asJava
          ).asJava
        ).asJava
      )
      val versionConfig = ConfigFactory.parseMap(
        Map("version" -> sentryConfig.serviceVersion.value).asJava
      )

      val Failure(exception) = SentryConfig[Try](config, versionConfig.some)

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if 'services.sentry.enabled' is 'true' but 'service-name' is invalid" in {
      val sentryConfig = sentryConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map[String, AnyRef](
          "service-name" -> "",
          "services" -> Map(
            "sentry" -> Map(
              "enabled"     -> "true",
              "dsn"         -> sentryConfig.baseUrl.value,
              "environment" -> sentryConfig.environmentName.value
            ).asJava
          ).asJava
        ).asJava
      )
      val versionConfig = ConfigFactory.parseMap(
        Map("version" -> sentryConfig.serviceVersion.value).asJava
      )

      val Failure(exception) = SentryConfig[Try](config, versionConfig.some)

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if 'services.sentry.enabled' is 'true' but 'services.sentry.environment' is invalid" in {
      val sentryConfig = sentryConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map[String, AnyRef](
          "service-name" -> sentryConfig.serviceName.value,
          "services" -> Map(
            "sentry" -> Map(
              "enabled"     -> "true",
              "dsn"         -> sentryConfig.baseUrl.value,
              "environment" -> ""
            ).asJava
          ).asJava
        ).asJava
      )
      val versionConfig = ConfigFactory.parseMap(
        Map("version" -> sentryConfig.serviceVersion.value).asJava
      )

      val Failure(exception) = SentryConfig[Try](config, versionConfig.some)

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if 'services.sentry.enabled' is 'true' but 'version' is invalid" in {
      val sentryConfig = sentryConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map[String, AnyRef](
          "service-name" -> sentryConfig.serviceName.value,
          "services" -> Map(
            "sentry" -> Map(
              "enabled"     -> "true",
              "dsn"         -> sentryConfig.baseUrl.value,
              "environment" -> sentryConfig.environmentName.value
            ).asJava
          ).asJava
        ).asJava
      )
      val versionConfig = ConfigFactory.parseMap(
        Map("version" -> "").asJava
      )

      val Failure(exception) = SentryConfig[Try](config, versionConfig.some)

      exception shouldBe a[ConfigLoadingException]
    }
  }
}
