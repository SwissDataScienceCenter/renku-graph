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
import ch.datascience.generators.CommonGraphGenerators.cliVersions
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.{EntityTypes, JsonLD}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class IOTriplesRemoverSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "removeAllTriples" should {

    "remove all the triples from the storage except for CLI version" in new TestCase {

      val cliVersionJsonLD = JsonLD.entity(
        id = CliVersionJsonLD.id,
        types = EntityTypes.of(CliVersionJsonLD.objectType),
        CliVersionJsonLD.version -> JsonLD.fromString(cliVersions.generateOne.toString)
      )

      val reprovisioningJsonLD = JsonLD.entity(
        id = ReProvisioningJsonLD.id,
        types = EntityTypes.of(ReProvisioningJsonLD.objectType),
        ReProvisioningJsonLD.reProvisioningStatus -> JsonLD.fromBoolean(true)
      )

      loadToStore(
        randomDataSetCommit,
        randomDataSetCommit,
        cliVersionJsonLD,
        reprovisioningJsonLD
      )

      rdfStoreSize should be > 0

      triplesRemover
        .removeAllTriples()
        .unsafeRunSync() shouldBe ((): Unit)

      val totalNumberOfTriples =
        cliVersionJsonLD.properties.size + 1 + reprovisioningJsonLD.properties.size + 1 // +1 for rdf:type
      rdfStoreSize shouldBe totalNumberOfTriples
    }
  }

  private trait TestCase {
    private val removalBatchSize      = positiveLongs(max = 100000).generateOne
    val logger                        = TestLogger[IO]()
    private val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    private val sparqlTimeRecorder    = new SparqlQueryTimeRecorder(executionTimeRecorder)
    val triplesRemover                = new IOTriplesRemover(removalBatchSize, rdfStoreConfig, logger, sparqlTimeRecorder)
  }
}
