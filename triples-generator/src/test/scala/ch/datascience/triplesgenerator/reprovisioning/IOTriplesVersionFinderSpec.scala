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

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.CliVersion
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class IOTriplesVersionFinderSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  private implicit lazy val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne

  "triplesUpToDate" should {

    "return true if CLI Version in the triples store matches the current version of Renku" in new TestCase {

      loadToStore(cliVersionOnTG(currentCliVersion))

      triplesVersionFinder.triplesUpToDate.unsafeRunSync() shouldBe true

      logger.loggedOnly(Warn(s"cli version find finished${executionTimeRecorder.executionTimeInfo}"))
    }

    "return false if CLI Version in the triples store does not match the current version of Renku" in new TestCase {

      loadToStore(cliVersionOnTG(cliVersions.generateOne))

      triplesVersionFinder.triplesUpToDate.unsafeRunSync() shouldBe false
    }

    "return false if CLI Version cannot be found in the triples store" in new TestCase {
      triplesVersionFinder.triplesUpToDate.unsafeRunSync() shouldBe false
    }
  }

  private trait TestCase {
    val currentCliVersion     = cliVersions.generateOne
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder(logger)
    private val timeRecorder  = new SparqlQueryTimeRecorder(executionTimeRecorder)
    val triplesVersionFinder  = new IOTriplesVersionFinder(rdfStoreConfig, currentCliVersion, logger, timeRecorder)
  }

  private def cliVersionOnTG(version: CliVersion) =
    JsonLD.entity(
      EntityId of renkuBaseUrl / "cli-version",
      EntityTypes of renku / "CliVersion",
      renku / "version" -> version.asJsonLD
    )
}
