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

package io.renku.triplesgenerator.reprovisioning

import cats.effect.IO
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.generators.Generators._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.generators.VersionGenerators.renkuVersionPairs
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TriplesRemoverSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "removeAllTriples" should {

    "remove all the triples from the storage except for CLI version" in new TestCase {
      val versionPair = renkuVersionPairs.generateOne
      val versionPairJsonLD = JsonLD.entity(
        id = RenkuVersionPairJsonLD.id,
        types = EntityTypes.of(RenkuVersionPairJsonLD.objectType),
        RenkuVersionPairJsonLD.cliVersion    -> JsonLD.fromString(versionPair.cliVersion.toString),
        RenkuVersionPairJsonLD.schemaVersion -> JsonLD.fromString(versionPair.schemaVersion.toString)
      )

      val reProvisioningJsonLD = JsonLD.entity(
        id = ReProvisioningJsonLD.id,
        types = EntityTypes.of(ReProvisioningJsonLD.objectType),
        ReProvisioningJsonLD.reProvisioningStatus -> JsonLD.fromBoolean(true)
      )

      loadToStore(
        anyProjectEntities.generateOne.asJsonLD,
        anyProjectEntities.generateOne.asJsonLD,
        versionPairJsonLD,
        reProvisioningJsonLD
      )

      val versionRelatedTriplesCount =
        versionPairJsonLD.properties.size + 1 + reProvisioningJsonLD.properties.size + 1 // +1 for rdf:type

      rdfStoreSize should be > versionRelatedTriplesCount

      triplesRemover
        .removeAllTriples()
        .unsafeRunSync() shouldBe ((): Unit)

      rdfStoreSize shouldBe versionRelatedTriplesCount
    }
  }

  private trait TestCase {
    private val removalBatchSize      = positiveLongs(max = 100000).generateOne
    private val logger                = TestLogger[IO]()
    private val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    private val sparqlTimeRecorder    = new SparqlQueryTimeRecorder(executionTimeRecorder)
    val triplesRemover = new TriplesRemoverImpl(removalBatchSize, rdfStoreConfig, logger, sparqlTimeRecorder)
  }
}
