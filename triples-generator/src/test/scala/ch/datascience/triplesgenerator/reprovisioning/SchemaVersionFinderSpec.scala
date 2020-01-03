/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.triplesgenerator.config.TriplesGeneration.{RemoteTriplesGeneration, RenkuLog}
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

class SchemaVersionFinderSpec extends WordSpec with MockFactory {

  "apply" should {

    val schemaVersion = schemaVersions.generateOne

    s"return 'services.triples-generator.schema-version' config value if TriplesGeneration is $RemoteTriplesGeneration" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "triples-generator" -> Map(
              "schema-version" -> schemaVersion.toString
            ).asJava
          ).asJava
        ).asJava
      )

      SchemaVersionFinder[Try](triplesGeneration  = RemoteTriplesGeneration,
                               renkuVersionFinder = context.pure(schemaVersions.generateOne),
                               config             = config) shouldBe Success(schemaVersion)
    }

    s"call 'renku --version' if TriplesGeneration is $RenkuLog" in {
      SchemaVersionFinder[Try](triplesGeneration  = RenkuLog,
                               renkuVersionFinder = context.pure(schemaVersion),
                               config             = ConfigFactory.empty()) shouldBe Success(schemaVersion)
    }
  }

  private val context = MonadError[Try, Throwable]
}
