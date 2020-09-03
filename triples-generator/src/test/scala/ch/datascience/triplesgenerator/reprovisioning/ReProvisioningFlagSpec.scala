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

import java.lang.Thread.sleep

import cats.effect.IO
import cats.effect.concurrent.Ref
import ch.datascience.generators.CommonGraphGenerators.renkuBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.language.postfixOps

class ReProvisioningFlagSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore {

  "currentlyReProvisioning" should {

    "reflect the state of the re-provisioning flag in the DB" in new TestCase {
      flagSetter.setUnderReProvisioningFlag().unsafeRunSync() shouldBe ((): Unit)

      reProvisioning.currentlyReProvisioning.unsafeRunSync() shouldBe true
    }

    "cache the value of the flag in DB once it's set to false" in new TestCase {
      flagSetter.setUnderReProvisioningFlag().unsafeRunSync() shouldBe ((): Unit)

      reProvisioning.currentlyReProvisioning.unsafeRunSync() shouldBe true
      reProvisioning.currentlyReProvisioning.unsafeRunSync() shouldBe true

      flagSetter.clearUnderReProvisioningFlag.unsafeRunSync() shouldBe ((): Unit)
      reProvisioning.currentlyReProvisioning.unsafeRunSync()  shouldBe false

      flagSetter.setUnderReProvisioningFlag().unsafeRunSync() shouldBe ((): Unit)
      sleep(cacheRefresh.toMillis - 500)
      reProvisioning.currentlyReProvisioning.unsafeRunSync() shouldBe false

      sleep(500 + 100)
      reProvisioning.currentlyReProvisioning.unsafeRunSync() shouldBe true
    }
  }

  private trait TestCase {
    val cacheRefresh         = 1 second
    private val renkuBaseUrl = renkuBaseUrls.generateOne
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    private val flagCheck    = Ref.of[IO, Boolean](true).unsafeRunSync()
    val flagSetter           = new ReProvisioningFlagSetterImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)

    val reProvisioning = new ReProvisioningFlagImpl(rdfStoreConfig, logger, timeRecorder, cacheRefresh, flagCheck)
  }
}
