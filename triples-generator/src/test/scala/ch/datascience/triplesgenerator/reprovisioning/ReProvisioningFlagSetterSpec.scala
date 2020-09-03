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
import ch.datascience.generators.CommonGraphGenerators.renkuBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ReProvisioningFlagSetterSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore {

  "setUnderReProvisioningFlag" should {

    "insert the ReProvisioningJsonLD object" in new TestCase {

      findFlag shouldBe false

      flagSetter.setUnderReProvisioningFlag().unsafeRunSync() shouldBe ((): Unit)

      findFlag shouldBe true
    }
  }

  "clearUnderReProvisioningFlag" should {
    "completely remove the ReProvisioning object" in new TestCase {
      flagSetter.setUnderReProvisioningFlag().unsafeRunSync() shouldBe ((): Unit)

      findFlag shouldBe true

      flagSetter.clearUnderReProvisioningFlag.unsafeRunSync() should be((): Unit)

      findFlag shouldBe false
    }

    "not throw an error if the ReProvisioning object isn't there" in new TestCase {

      findFlag shouldBe false

      flagSetter.clearUnderReProvisioningFlag.unsafeRunSync() should be((): Unit)

      findFlag shouldBe false
    }
  }

  private trait TestCase {
    private val renkuBaseUrl = renkuBaseUrls.generateOne
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))

    val flagSetter = new ReProvisioningFlagSetterImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }

  private def findFlag: Boolean =
    runQuery(s"""|SELECT DISTINCT ?flag
                 |WHERE {
                 |  ?id rdf:type renku:ReProvisioning;
                 |      renku:currentlyReProvisioning ?flag
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("flag").toBoolean)
      .headOption
      .getOrElse(false)
}
