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
import eu.timepit.refined.auto._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningJsonLD.{Running, objectType}
import ch.datascience.triplesgenerator.subscriptions.Subscriber
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.language.postfixOps

class ReProvisioningStatusSpec extends AnyWordSpec with should.Matchers with MockFactory with InMemoryRdfStore {

  "setRunning" should {

    "insert the ReProvisioningJsonLD object" in new TestCase {

      findStatus shouldBe None

      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ((): Unit)

      findStatus shouldBe Some(Running.toString)
    }
  }

  "clear" should {

    "completely remove the ReProvisioning object" in new TestCase {
      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ((): Unit)

      findStatus shouldBe Some(Running.toString)

      expectNotificationSent

      reProvisioningStatus.clear().unsafeRunSync() should be((): Unit)

      findStatus shouldBe None
    }

    "not throw an error if the ReProvisioning object isn't there" in new TestCase {

      findStatus shouldBe None

      expectNotificationSent

      reProvisioningStatus.clear().unsafeRunSync() should be((): Unit)

      findStatus shouldBe None
    }
  }

  "isReProvisioning" should {

    "reflect the state of the re-provisioning status in the DB" in new TestCase {
      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ((): Unit)

      reProvisioningStatus.isReProvisioning.unsafeRunSync() shouldBe true
    }

    "cache the value of the flag in DB once it's set to false" in new TestCase {
      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ((): Unit)

      reProvisioningStatus.isReProvisioning.unsafeRunSync() shouldBe true
      reProvisioningStatus.isReProvisioning.unsafeRunSync() shouldBe true

      clearStatus()
      reProvisioningStatus.isReProvisioning.unsafeRunSync() shouldBe false

      reProvisioningStatus.setRunning().unsafeRunSync() shouldBe ((): Unit)
      sleep(cacheRefreshInterval.toMillis - 500)
      reProvisioningStatus.isReProvisioning.unsafeRunSync() shouldBe false

      sleep(500 + 100)
      reProvisioningStatus.isReProvisioning.unsafeRunSync() shouldBe true
    }

    "check if re-provisioning is done and notify availability to event-log" in new TestCase {
      reProvisioningStatus.setRunning().unsafeRunSync()     shouldBe ((): Unit)
      reProvisioningStatus.isReProvisioning.unsafeRunSync() shouldBe true

      clearStatus()

      expectNotificationSent
      sleep(statusRefreshInterval.toMillis)
      reProvisioningStatus.isReProvisioning.unsafeRunSync() shouldBe false
    }
  }

  private trait TestCase {
    val cacheRefreshInterval            = 1 second
    val statusRefreshInterval           = 1 second
    private val renkuBaseUrl            = renkuBaseUrls.generateOne
    private val logger                  = TestLogger[IO]()
    private val timeRecorder            = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    private val statusCacheCheckTimeRef = Ref.of[IO, Long](0L).unsafeRunSync()
    val subscriber                      = mock[Subscriber[IO]]

    val reProvisioningStatus = new ReProvisioningStatusImpl(subscriber,
                                                            rdfStoreConfig,
                                                            renkuBaseUrl,
                                                            logger,
                                                            timeRecorder,
                                                            statusRefreshInterval,
                                                            cacheRefreshInterval,
                                                            statusCacheCheckTimeRef)

    def expectNotificationSent =
      (subscriber.notifyAvailability _)
        .expects()
        .returning(IO.unit)
  }

  private def findStatus: Option[String] =
    runQuery(s"""|SELECT DISTINCT ?status
                 |WHERE {
                 |  ?id rdf:type renku:ReProvisioning;
                 |      renku:reProvisioningStatus ?status
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("status"))
      .headOption

  private def clearStatus(): Unit =
    runUpdate {
      SparqlQuery(
        name = "re-provisioning - status remove",
        Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"),
        s"""|DELETE { ?s ?p ?o }
            |WHERE {
            | ?s ?p ?o;
            |    rdf:type <$objectType> .
            |}
            |""".stripMargin
      )
    }.unsafeRunSync()
}
