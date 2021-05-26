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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.generators.VersionGenerators.renkuVersionPairs
import ch.datascience.graph.model.RenkuVersionPair
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD}
import org.scalatest._
import matchers._
import org.scalatest.wordspec.AnyWordSpec

class RenkuVersionFinderSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  private implicit lazy val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne

  "find" should {

    "return the Version pair in the triples store" in new TestCase {

      loadToStore(versionPairOnTG(currentVersionPair))

      versionPairFinder.find().unsafeRunSync() shouldBe currentVersionPair.some

      logger.loggedOnly(Warn(s"version pair find finished${executionTimeRecorder.executionTimeInfo}"))
    }

    "return None if there are no Version pair in the triple store" in new TestCase {

      versionPairFinder.find().unsafeRunSync() shouldBe None

      logger.loggedOnly(Warn(s"version pair find finished${executionTimeRecorder.executionTimeInfo}"))
    }

    "return an IllegalStateException if there are multiple Version Pairs" in new TestCase {
      val secondVersionPair = renkuVersionPairs.generateOne
      loadToStore(versionPairOnTG(currentVersionPair))
      loadToStore(versionPairOnTG(secondVersionPair))

      intercept[IllegalStateException] {
        versionPairFinder.find().unsafeRunSync()
      }.getMessage should startWith("Too many Version pair found:")
    }
  }

  private trait TestCase {
    val currentVersionPair    = renkuVersionPairs.generateOne
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder(logger)
    private val timeRecorder  = new SparqlQueryTimeRecorder(executionTimeRecorder)
    val versionPairFinder     = new RenkuVersionPairFinderImpl[IO](rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }

  private def versionPairOnTG(version: RenkuVersionPair) =
    JsonLD.entity(
      EntityId of renkuBaseUrl / "version-pair",
      EntityTypes of renku / "VersionPair",
      renku / "schemaVersion" -> version.schemaVersion.asJsonLD,
      renku / "cliVersion"    -> version.cliVersion.asJsonLD
    )
}
