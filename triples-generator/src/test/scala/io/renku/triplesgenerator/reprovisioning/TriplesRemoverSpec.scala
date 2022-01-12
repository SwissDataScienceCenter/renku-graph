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

package io.renku.triplesgenerator.reprovisioning

import cats.effect.IO
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.generators.Generators._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.generators.VersionGenerators.renkuVersionPairs
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TriplesRemoverSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  "removeAllTriples" should {

    "remove all the triples from the storage except for CLI version" in new TestCase {
      loadToStore(
        anyProjectEntities.generateOne.asJsonLD,
        anyProjectEntities.generateOne.asJsonLD,
        renkuVersionPairs.generateOne.asJsonLD,
        ReProvisioningInfo(ReProvisioningInfo.Status.Running, microserviceBaseUrls.generateOne).asJsonLD
      )

      val graphMetadataTriples = runQuery {
        """|SELECT DISTINCT ?s ?p ?o 
           |WHERE {
           |  ?s a ?type
           |  FILTER (?type IN (renku:VersionPair, renku:ReProvisioning))
           |  ?s ?p ?o
           |}
           |""".stripMargin
      }.unsafeRunSync()

      rdfStoreSize should be > graphMetadataTriples.size

      triplesRemover.removeAllTriples().unsafeRunSync() shouldBe ()

      rdfStoreSize shouldBe graphMetadataTriples.size
    }
  }

  private trait TestCase {
    private val removalBatchSize = positiveLongs(max = 100000).generateOne
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    private val sparqlTimeRecorder    = new SparqlQueryTimeRecorder(executionTimeRecorder)
    val triplesRemover                = new TriplesRemoverImpl(removalBatchSize, rdfStoreConfig, sparqlTimeRecorder)
  }
}
