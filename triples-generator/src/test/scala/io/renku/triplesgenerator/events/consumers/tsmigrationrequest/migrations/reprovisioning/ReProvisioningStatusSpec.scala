/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import ReProvisioningInfo.Status.Running
import cats.effect.kernel.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Temporal}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.CommonGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._

class ReProvisioningStatusSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with AsyncMockFactory
    with should.Matchers {

  "underReProvisioning" should {

    "reflect the state of the re-provisioning info in the DB" in migrationsDSConfig.use { implicit mcc =>
      val controller           = microserviceBaseUrls.generateOne
      val reProvisioningStatus = createReProvisioningStatus

      reProvisioningStatus.setRunning(on = controller).assertNoException >>
        reProvisioningStatus.underReProvisioning().asserting(_ shouldBe true)
    }

    "cache the value of the flag in the DB once it's set to false" in migrationsDSConfig.use { implicit mcc =>
      val controller           = microserviceBaseUrls.generateOne
      val reProvisioningStatus = createReProvisioningStatus
      for {
        _ <- reProvisioningStatus.setRunning(on = controller).assertNoException

        _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe true)
        _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe true)

        _ <- clearStatus
        _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe false)

        _ <- reProvisioningStatus.setRunning(on = controller).assertNoException
        _ <- Temporal[IO].sleep((cacheRefreshInterval.toMillis - cacheRefreshInterval.toMillis * 2 / 3).millis)
        _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe false)

        _ <- Temporal[IO].sleep((cacheRefreshInterval.toMillis * 2 / 3 + 100).millis)
        _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe true)
      } yield Succeeded
    }

    "check if re-provisioning is done and notify availability to event-log" in migrationsDSConfig.use { implicit mcc =>
      val controller           = microserviceBaseUrls.generateOne
      val reProvisioningStatus = createReProvisioningStatus
      for {
        _ <- reProvisioningStatus.setRunning(on = controller).assertNoException
        _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe true)

        _ <- registerSubscriptions(reProvisioningStatus).map(expectNotificationSent(_: _*))

        _ <- clearStatus

        _ <- Temporal[IO].sleep((statusRefreshInterval + (500 millis)).toMillis.millis)

        _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe false)
      } yield Succeeded
    }
  }

  "setRunning" should {

    "insert the ReProvisioningJsonLD object" in migrationsDSConfig.use { implicit mcc =>
      for {
        _ <- findInfo.asserting(_ shouldBe None)

        controller = microserviceBaseUrls.generateOne
        _ <- createReProvisioningStatus.setRunning(on = controller).assertNoException

        _ <- findInfo.asserting(_ shouldBe (Running.value, controller.value).some)
      } yield Succeeded
    }
  }

  "clear" should {

    "completely remove the ReProvisioning object" in migrationsDSConfig.use { implicit mcc =>
      val controller           = microserviceBaseUrls.generateOne
      val reProvisioningStatus = createReProvisioningStatus

      for {
        _ <- reProvisioningStatus.setRunning(on = controller).assertNoException

        _ <- findInfo.asserting(_ shouldBe (Running.value, controller.value).some)

        _ <- registerSubscriptions(reProvisioningStatus).map(expectNotificationSent(_: _*))

        _ <- reProvisioningStatus.clear().assertNoException

        _ <- findInfo.asserting(_ shouldBe None)
      } yield Succeeded
    }

    "not throw an error if the ReProvisioning object isn't there" in migrationsDSConfig.use { implicit mcc =>
      val reProvisioningStatus = createReProvisioningStatus
      for {
        _ <- findInfo.asserting(_ shouldBe None)

        _ <- registerSubscriptions(reProvisioningStatus).map(expectNotificationSent(_: _*))

        _ <- reProvisioningStatus.clear().assertNoException

        _ <- findInfo.asserting(_ shouldBe None)
      } yield Succeeded
    }
  }

  "findReProvisioningController" should {

    "return Controller if exists in the KG" in migrationsDSConfig.use { implicit mcc =>
      val controller           = microserviceBaseUrls.generateOne
      val reProvisioningStatus = createReProvisioningStatus
      reProvisioningStatus.setRunning(on = controller).assertNoException >>
        reProvisioningStatus.findReProvisioningService().asserting(_ shouldBe controller.some)
    }

    "return nothing if there's no Controller info in the KG" in migrationsDSConfig.use { implicit mcc =>
      createReProvisioningStatus.findReProvisioningService().asserting(_ shouldBe None)
    }
  }

  "registerForNotification" should {

    "register the given Subscription Mechanism for the notification about done re-provisioning" in migrationsDSConfig
      .use { implicit mcc =>
        val controller           = microserviceBaseUrls.generateOne
        val reProvisioningStatus = createReProvisioningStatus
        for {
          _ <- reProvisioningStatus.setRunning(on = controller).assertNoException

          _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe true)

          subscription = mock[SubscriptionMechanism[IO]]
          _ <- reProvisioningStatus.registerForNotification(subscription).assertNoException
          _ <- registerSubscriptions(reProvisioningStatus).map(subscription :: _).map(expectNotificationSent(_: _*))

          _ <- clearStatus

          _ <- Temporal[IO].sleep((statusRefreshInterval + (500 millis)).toMillis.millis)

          _ <- reProvisioningStatus.underReProvisioning().asserting(_ shouldBe false)
        } yield Succeeded
      }
  }

  private def registerSubscriptions(reProvisioningStatus: ReProvisioningStatus[IO]) = {
    val subscriptions = List(mock[SubscriptionMechanism[IO]], mock[SubscriptionMechanism[IO]])
    subscriptions.traverse_(reProvisioningStatus.registerForNotification).as(subscriptions)
  }

  private lazy val cacheRefreshInterval  = 1 second
  private lazy val statusRefreshInterval = 1 second
  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def createReProvisioningStatus(implicit mcc: MigrationsConnectionConfig) = {
    implicit val tr:       SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    implicit val renkuUrl: RenkuUrl                    = renkuUrls.generateOne
    val statusCacheCheckTimeRef = Ref.unsafe[IO, Long](0L)
    val subscriptionsRegistry   = Ref.unsafe[IO, List[SubscriptionMechanism[IO]]](Nil)
    new ReProvisioningStatusImpl[IO](mcc,
                                     statusRefreshInterval,
                                     cacheRefreshInterval,
                                     subscriptionsRegistry,
                                     statusCacheCheckTimeRef
    )
  }

  private def expectNotificationSent(subscriptions: SubscriptionMechanism[IO]*): Unit =
    subscriptions foreach { subscription =>
      (subscription.renewSubscription _)
        .expects()
        .returning(IO.unit)
    }

  private def findInfo(implicit mcc: MigrationsConnectionConfig): IO[Option[(String, String)]] =
    runSelect(
      SparqlQuery.of(
        "fetch re-provisioning info",
        Prefixes of renku -> "renku",
        s"""|SELECT DISTINCT ?status ?controllerUrl
            |WHERE {
            |  ?id a renku:ReProvisioning;
            |        renku:status ?status;
            |        renku:controllerUrl ?controllerUrl
            |}
            |""".stripMargin
      )
    ).map(_.map(row => row("status") -> row("controllerUrl")).headOption)

  private def clearStatus(implicit mcc: MigrationsConnectionConfig): IO[Unit] =
    runUpdate(
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
    )
}
