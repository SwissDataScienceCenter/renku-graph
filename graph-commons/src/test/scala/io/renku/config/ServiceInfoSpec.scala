/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.config

import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import io.renku.generators.CommonGraphGenerators.{serviceNames, serviceVersions}
import io.renku.generators.Generators.Implicits._
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class ServiceVersionSpec extends AnyWordSpec with should.Matchers with IOSpec {

  "readFromConfig" should {

    "read the value from the 'version' property in the config and turn it into ServiceVersion" in {
      val serviceVersion = serviceVersions.generateOne
      val versionConfig  = ConfigFactory.parseMap(Map("version" -> serviceVersion.show).asJava)

      ServiceVersion.readFromConfig[Try](versionConfig) shouldBe serviceVersion.pure[Try]
    }

    "fail if there's no 'version' property in the config" in {
      val Failure(exception) = ServiceVersion.readFromConfig[Try](ConfigFactory.empty())
      exception.getMessage shouldBe "Key not found: 'version'."
    }

    "fail if 'version' property in the config has illegal value" in {
      val versionConfig = ConfigFactory.parseMap(Map("version" -> "").asJava)

      val Failure(exception) = ServiceVersion.readFromConfig[Try](versionConfig)

      exception.getMessage should startWith("Cannot convert ''")
    }
  }
}

class ServiceNameSpec extends AnyWordSpec with should.Matchers with IOSpec {

  "readFromConfig" should {

    "read the value from the 'service-name' property in the config and turn it into ServiceVersion" in {
      val serviceName = serviceNames.generateOne
      val config      = ConfigFactory.parseMap(Map("service-name" -> serviceName.show).asJava)

      ServiceName.readFromConfig[Try](config) shouldBe serviceName.pure[Try]
    }

    "fail if there's no 'service-name' property in the config" in {
      val Failure(exception) = ServiceName.readFromConfig[Try](ConfigFactory.empty())
      exception.getMessage shouldBe "Key not found: 'service-name'."
    }

    "fail if 'service-name' property in the config has illegal value" in {
      val config = ConfigFactory.parseMap(Map("service-name" -> "").asJava)

      val Failure(exception) = ServiceName.readFromConfig[Try](config)

      exception.getMessage should startWith("Cannot convert ''")
    }
  }
}
