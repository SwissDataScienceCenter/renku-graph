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

package ch.datascience.graph.model

import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.graph.model.GraphModelGenerators._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class SchemaVersionSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "apply" should {

    "return a SchemaVersion if there's a value for 'schema-version'" in {
      forAll(schemaVersions) { schemaVersion =>
        val config = ConfigFactory.parseMap(
          Map(
            "schema-version" -> schemaVersion.toString
          ).asJava
        )

        SchemaVersion[Try](config) shouldBe Success(schemaVersion)
      }
    }

    "fail if there's no value for the 'schema-version'" in {
      val Failure(exception) = SchemaVersion[Try](ConfigFactory.empty())
      exception shouldBe an[ConfigLoadingException]
    }
  }
}
