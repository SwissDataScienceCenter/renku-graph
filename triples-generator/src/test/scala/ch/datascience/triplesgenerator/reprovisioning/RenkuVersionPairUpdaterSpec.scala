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
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.{CliVersion, SchemaVersion}
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.generators.VersionGenerators._
import ch.datascience.graph.model.RenkuVersionPair
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RenkuVersionPairUpdaterSpec extends AnyWordSpec with InMemoryRdfStore with Matchers {
  "update" should {
    "create a renku:VersionPair with the given version pair" in new TestCase {
      findPairInDb shouldBe Set.empty

      renkuVersionPairUpdater.update(currentRenkuVersionPair).unsafeRunSync()

      findPairInDb shouldBe Set(currentRenkuVersionPair)

      renkuVersionPairUpdater.update(newVersionCompatibilityPairs).unsafeRunSync()

      findPairInDb shouldBe Set(newVersionCompatibilityPairs)

    }
  }

  private trait TestCase {
    val currentRenkuVersionPair      = renkuVersionPairs.generateOne
    private val renkuBaseUrl         = renkuBaseUrls.generateOne
    private val logger               = TestLogger[IO]()
    private val timeRecorder         = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val newVersionCompatibilityPairs = renkuVersionPairs.generateOne

    val renkuVersionPairUpdater = new RenkuVersionPairUpdaterImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)

    def findPairInDb: Set[RenkuVersionPair] =
      runQuery(s"""|SELECT DISTINCT ?schemaVersion ?cliVersion
                   |WHERE {
                   |   ?id rdf:type renku:VersionPair;
                   |       renku:schemaVersion ?schemaVersion ;
                   |       renku:cliVersion ?cliVersion.
                   |}
                   |""".stripMargin)
        .unsafeRunSync()
        .map(row => RenkuVersionPair(CliVersion(row("cliVersion")), SchemaVersion(row("schemaVersion"))))
        .toSet
  }
}
