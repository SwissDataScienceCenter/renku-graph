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

package io.renku.triplesgenerator.config

import com.typesafe.config.ConfigFactory
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{blankStrings, nonEmptyStrings}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

class RenkuPythonDevVersionConfigSpec extends AnyWordSpec with should.Matchers {

  "apply" should {

    "return Some(version) if there is a value set" in {
      val version = nonEmptyStrings().generateOne
      val config = ConfigFactory.parseMap(
        Map("renku-python-dev-version" -> version).asJava
      )
      RenkuPythonDevVersionConfig[Try](config) shouldBe Success(Some(RenkuPythonDevVersion(version)))
    }

    "return None if there is no entry" in {
      RenkuPythonDevVersionConfig[Try](ConfigFactory.empty) shouldBe Success(None)
    }

    "return None if there is no value set" in {
      val config = ConfigFactory.parseMap(
        Map("renku-python-dev-version" -> null).asJava
      )
      RenkuPythonDevVersionConfig[Try](config) shouldBe Success(None)
    }

    "return None if there is an empty string" in {

      val config = ConfigFactory.parseMap(
        Map("renku-python-dev-version" -> blankStrings().generateOne).asJava
      )
      RenkuPythonDevVersionConfig[Try](config) shouldBe Success(None)
    }
  }
}
