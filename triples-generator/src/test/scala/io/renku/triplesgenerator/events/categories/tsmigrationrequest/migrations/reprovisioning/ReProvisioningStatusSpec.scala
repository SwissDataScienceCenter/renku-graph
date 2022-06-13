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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.reprovisioning

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.reprovisioning.ReProvisioningInfo.Status.Running
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.duration._

class ReProvisioningStatusSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with MockFactory
    with InMemoryRdfStore {

  "underReProvisioning" should {

    "reflect the state of the re-provisioning info in the DB" in new TestCase {
      val controller = microserviceBaseUrls.generateOne

      reProvisioningStatus.setRunning(on = controller).unsafeRunSync() shouldBe ()

      reProvisioningStatus.underReProvisioning().unsafeRunSync() shouldBe true
    }

    "cache the value of the flag in the DB once it's set to false" in new TestCase {
      val controller = microserviceBaseUrls.generateOne
      reProvisioningStatus.setRunning(on = controller).unsafeRunSync() shouldBe ()

      reProvisioningStatus.underReProvisioning().unsafeRunSync() shouldBe true
      reProvisioningStatus.underReProvisioning().unsafeRunSync() shouldBe true

      clearStatus()
      reProvisioningStatus.underReProvisioning().unsafeRunSync() shouldBe false

      reProvisioningStatus.setRunning(on = controller).unsafeRunSync() shouldBe ()
      sleep(cacheRefreshInterval.toMillis - cacheRefreshInterval.toMillis * 2 / 3)
      reProvisioningStatus.underReProvisioning().unsafeRunSync() shouldBe false

      sleep(cacheRefreshInterval.toMillis * 2 / 3 + 100)
      reProvisioningStatus.underReProvisioning().unsafeRunSync() shouldBe true
    }

    "check if re-provisioning is done and notify availability to event-log" in new TestCase {
      val controller = microserviceBaseUrls.generateOne
      reProvisioningStatus.setRunning(on = controller).unsafeRunSync() shouldBe ()
      reProvisioningStatus.underReProvisioning().unsafeRunSync()       shouldBe true

      expectNotificationSent()

      clearStatus()

      sleep((statusRefreshInterval + (500 millis)).toMillis)

      reProvisioningStatus.underReProvisioning().unsafeRunSync() shouldBe false
    }
  }

  "setRunning" should {

    "insert the ReProvisioningJsonLD object" in new TestCase {

      findInfo shouldBe None

      val controller = microserviceBaseUrls.generateOne
      reProvisioningStatus.setRunning(on = controller).unsafeRunSync() shouldBe ()

      findInfo shouldBe (Running.value, controller.value).some
    }
  }

  "clear" should {

    "completely remove the ReProvisioning object" in new TestCase {
      val controller = microserviceBaseUrls.generateOne

      reProvisioningStatus.setRunning(on = controller).unsafeRunSync() shouldBe ()

      findInfo shouldBe (Running.value, controller.value).some

      expectNotificationSent()

      reProvisioningStatus.clear().unsafeRunSync() shouldBe ()

      findInfo shouldBe None
    }

    "not throw an error if the ReProvisioning object isn't there" in new TestCase {

      findInfo shouldBe None

      expectNotificationSent()

      reProvisioningStatus.clear().unsafeRunSync() shouldBe ()

      findInfo shouldBe None
    }
  }

  "findReProvisioningController" should {

    "return Controller if exists in the KG" in new TestCase {
      val controller = microserviceBaseUrls.generateOne
      reProvisioningStatus.setRunning(on = controller).unsafeRunSync() shouldBe ()

      reProvisioningStatus.findReProvisioningService().unsafeRunSync() shouldBe controller.some
    }

    "return nothing if there's no Controller info in the KG" in new TestCase {
      reProvisioningStatus.findReProvisioningService().unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val cacheRefreshInterval  = 1 second
    val statusRefreshInterval = 1 second
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    private implicit val renkuUrl:     RenkuUrl                    = renkuUrls.generateOne
    private val statusCacheCheckTimeRef = Ref.unsafe[IO, Long](0L)
    private val subscriptions           = List(mock[SubscriptionMechanism[IO]], mock[SubscriptionMechanism[IO]])
    val reProvisioningStatus = new ReProvisioningStatusImpl[IO](subscriptions,
                                                                rdfStoreConfig,
                                                                statusRefreshInterval,
                                                                cacheRefreshInterval,
                                                                statusCacheCheckTimeRef
    )

    def expectNotificationSent(): Unit = subscriptions foreach { subscription =>
      (subscription.renewSubscription _)
        .expects()
        .returning(IO.unit)
    }
  }

  private def findInfo: Option[(String, String)] = runQuery {
    s"""|SELECT DISTINCT ?status ?controllerUrl
        |WHERE {
        |  ?id a renku:ReProvisioning;
        |      renku:status ?status;
        |      renku:controllerUrl ?controllerUrl
        |}
        |""".stripMargin
  }.unsafeRunSync()
    .map(row => row("status") -> row("controllerUrl"))
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
