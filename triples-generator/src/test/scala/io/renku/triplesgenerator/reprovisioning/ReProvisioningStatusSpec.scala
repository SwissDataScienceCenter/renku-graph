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
import cats.effect.kernel.Ref
import eu.timepit.refined.auto._
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuBaseUrls
import io.renku.graph.model.RenkuBaseUrl
import io.renku.graph.model.Schemas._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.reprovisioning.ReProvisioningInfo.Status.Running
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.duration._
import scala.language.postfixOps

class ReProvisioningStatusSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with MockFactory
    with InMemoryRdfStore {

  "setRunning" should {

    "insert the ReProvisioningJsonLD object" in new TestCase {

      findStatus shouldBe None

      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ()

      findStatus shouldBe Some(Running.toString)
    }
  }

  "clear" should {

    "completely remove the ReProvisioning object" in new TestCase {
      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ()

      findStatus shouldBe Some(Running.toString)

      expectNotificationSent

      reProvisioningStatus.clear().unsafeRunSync() shouldBe ()

      findStatus shouldBe None
    }

    "not throw an error if the ReProvisioning object isn't there" in new TestCase {

      findStatus shouldBe None

      expectNotificationSent

      reProvisioningStatus.clear().unsafeRunSync() shouldBe ()

      findStatus shouldBe None
    }
  }

  "isReProvisioning" should {

    "reflect the state of the re-provisioning status in the DB" in new TestCase {
      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ()

      reProvisioningStatus.isReProvisioning().unsafeRunSync() shouldBe true
    }

    "cache the value of the flag in the DB once it's set to false" in new TestCase {
      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ()

      reProvisioningStatus.isReProvisioning().unsafeRunSync() shouldBe true
      reProvisioningStatus.isReProvisioning().unsafeRunSync() shouldBe true

      clearStatus()
      reProvisioningStatus.isReProvisioning().unsafeRunSync() shouldBe false

      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ()
      sleep(cacheRefreshInterval.toMillis - cacheRefreshInterval.toMillis * 2 / 3)
      reProvisioningStatus.isReProvisioning().unsafeRunSync() shouldBe false

      sleep(cacheRefreshInterval.toMillis * 2 / 3 + 100)
      reProvisioningStatus.isReProvisioning().unsafeRunSync() shouldBe true
    }

    "check if re-provisioning is done and notify availability to event-log" in new TestCase {
      reProvisioningStatus.setRunning().unsafeRunSync()       shouldBe ()
      reProvisioningStatus.isReProvisioning().unsafeRunSync() shouldBe true

      expectNotificationSent

      clearStatus()

      sleep((statusRefreshInterval + (500 millis)).toMillis)

      reProvisioningStatus.isReProvisioning().unsafeRunSync() shouldBe false
    }
  }

  private trait TestCase {
    val cacheRefreshInterval  = 1 second
    val statusRefreshInterval = 1 second
    private implicit val logger:       TestLogger[IO] = TestLogger[IO]()
    private implicit val renkuBaseUrl: RenkuBaseUrl   = renkuBaseUrls.generateOne
    private val timeRecorder            = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]())
    private val statusCacheCheckTimeRef = Ref.of[IO, Long](0L).unsafeRunSync()
    val eventConsumersRegistry          = mock[EventConsumersRegistry[IO]]

    val reProvisioningStatus = new ReProvisioningStatusImpl(eventConsumersRegistry,
                                                            rdfStoreConfig,
                                                            timeRecorder,
                                                            statusRefreshInterval,
                                                            cacheRefreshInterval,
                                                            statusCacheCheckTimeRef
    )

    def expectNotificationSent =
      (eventConsumersRegistry.renewAllSubscriptions _)
        .expects()
        .returning(IO.unit)
  }

  private def findStatus: Option[String] =
    runQuery(s"""|SELECT DISTINCT ?status
                 |WHERE {
                 |  ?id a renku:ReProvisioning;
                 |      renku:status ?status
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("status"))
      .headOption

  private def clearStatus(): Unit = runUpdate {
    SparqlQuery.of(
      name = "re-provisioning - status remove",
      Prefixes of renku -> "renku",
      s"""|DELETE { ?s ?p ?o }
          |WHERE {
          | ?s ?p ?o;
          |    a renku:ReProvisioning.
          |}
          |""".stripMargin
    )
  }.unsafeRunSync()
}
