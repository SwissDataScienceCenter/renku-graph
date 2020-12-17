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

package ch.datascience.triplesgenerator.init

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.triplesgenerator.config.TriplesGeneration.{RemoteTriplesGeneration, RenkuLog}
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

class CliVersionLoaderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "apply" should {

    val cliVersion = cliVersions.generateOne

    s"return 'services.triples-generator.cli-version' config value if TriplesGeneration is $RemoteTriplesGeneration" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "triples-generator" -> Map(
              "cli-version" -> cliVersion.toString
            ).asJava
          ).asJava
        ).asJava
      )

      CliVersionLoader[Try](triplesGeneration = RemoteTriplesGeneration,
                            renkuVersionFinder = cliVersions.generateOne.pure[Try],
                            config = config
      ) shouldBe Success(cliVersion)
    }

    s"call 'renku --version' if TriplesGeneration is $RenkuLog" in {
      CliVersionLoader[Try](triplesGeneration = RenkuLog,
                            renkuVersionFinder = cliVersion.pure[Try],
                            config = ConfigFactory.empty()
      ) shouldBe Success(cliVersion)
    }
  }
}
