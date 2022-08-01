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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import cats.Parallel
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.microservices.MicroserviceBaseUrl
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait ReProvisioningStatus[F[_]] {
  def underReProvisioning(): F[Boolean]
  def setRunning(on: MicroserviceBaseUrl): F[Unit]
  def clear():                     F[Unit]
  def findReProvisioningService(): F[Option[MicroserviceBaseUrl]]
  def registerForNotification(subscription: SubscriptionMechanism[F]): F[Unit]
}

private class ReProvisioningStatusImpl[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder](
    storeConfig:           MigrationsConnectionConfig,
    statusRefreshInterval: FiniteDuration,
    cacheRefreshInterval:  FiniteDuration,
    subscriptionsRegistry: Ref[F, List[SubscriptionMechanism[F]]],
    lastCacheCheckTimeRef: Ref[F, Long]
)(implicit renkuUrl:       RenkuUrl)
    extends TSClientImpl(storeConfig)
    with ReProvisioningStatus[F] {

  import io.renku.jsonld.syntax._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  private lazy val runningStatusCheckStarted = Ref.unsafe[F, Boolean](false)

  override def underReProvisioning(): F[Boolean] = isCacheExpired >>= {
    case true =>
      fetchStatus >>= {
        case Some(ReProvisioningInfo.Status.Running) => triggerPeriodicStatusCheck() map (_ => true)
        case _                                       => updateCacheCheckTime() map (_ => false)
      }
    case false => false.pure[F]
  }

  override def setRunning(on: MicroserviceBaseUrl): F[Unit] = upload(
    ReProvisioningInfo(ReProvisioningInfo.Status.Running, on).asJsonLD
  )

  override def clear(): F[Unit] = deleteFromDb() >> renewSubscriptions()

  override def findReProvisioningService(): F[Option[MicroserviceBaseUrl]] =
    queryExpecting[Option[MicroserviceBaseUrl]] {
      SparqlQuery.of(
        name = "re-provisioning - fetch controller",
        Prefixes of renku -> "renku",
        s"""|SELECT ?url ?id
            |WHERE { 
            |  ?entityId a renku:ReProvisioning;
            |              renku:controllerUrl ?url;
            |}
            |""".stripMargin
      )
    }(controllerUrlDecoder)

  override def registerForNotification(subscription: SubscriptionMechanism[F]): F[Unit] =
    subscriptionsRegistry.update(subscription :: _)

  private def deleteFromDb() = updateWithNoResult {
    SparqlQuery.of(
      name = "re-provisioning - status remove",
      Prefixes of renku -> "renku",
      s"""|DELETE { ?s ?p ?o }
          |WHERE {
          | ?s a renku:ReProvisioning;
          |    ?p ?o.
          |}
          |""".stripMargin
    )
  }

  private def isCacheExpired: F[Boolean] = for {
    lastCheckTime <- lastCacheCheckTimeRef.get
    currentTime   <- Temporal[F].monotonic.map(_.toMillis)
  } yield currentTime - lastCheckTime > cacheRefreshInterval.toMillis

  private def updateCacheCheckTime() = for {
    currentTime <- Temporal[F].monotonic
    _           <- lastCacheCheckTimeRef set currentTime.toMillis
  } yield ()

  private def triggerPeriodicStatusCheck(): F[Unit] =
    runningStatusCheckStarted.getAndSet(true) >>= {
      case false => Spawn[F].start(periodicStatusCheck).void
      case true  => ().pure[F]
    }

  private def periodicStatusCheck: F[Unit] = Temporal[F].delayBy(
    fetchStatus >>= {
      case Some(ReProvisioningInfo.Status.Running) => periodicStatusCheck
      case _                                       => (runningStatusCheckStarted set false) >> renewSubscriptions()
    },
    time = statusRefreshInterval
  )

  private def renewSubscriptions() = (subscriptionsRegistry.get >>= (_.parTraverse(_.renewSubscription()))).void

  private def fetchStatus: F[Option[ReProvisioningInfo.Status]] = queryExpecting[Option[ReProvisioningInfo.Status]] {
    SparqlQuery.of(
      name = "re-provisioning - fetch status",
      Prefixes of renku -> "renku",
      s"""|SELECT ?status
          |WHERE { 
          |  ?entityId a renku:ReProvisioning;
          |              renku:status ?status.
          |}
          |""".stripMargin
    )
  }(statusDecoder)

  private lazy val statusDecoder: Decoder[Option[ReProvisioningInfo.Status]] = {

    val ofStatuses: Decoder[ReProvisioningInfo.Status] =
      _.downField("status").downField("value").as[ReProvisioningInfo.Status]

    _.downField("results")
      .downField("bindings")
      .as(decodeList(ofStatuses))
      .map(_.headOption)
  }

  private lazy val controllerUrlDecoder: Decoder[Option[MicroserviceBaseUrl]] = {

    val ofControllers: Decoder[MicroserviceBaseUrl] =
      _.downField("url").downField("value").as[MicroserviceBaseUrl]

    _.downField("results")
      .downField("bindings")
      .as(decodeList(ofControllers))
      .map(_.headOption)
  }
}

object ReProvisioningStatus {

  private val CacheRefreshInterval:  FiniteDuration = 2 minutes
  private val StatusRefreshInterval: FiniteDuration = 15 seconds

  def apply[F[_]](implicit ev: ReProvisioningStatus[F]): ReProvisioningStatus[F] = ev

  def apply[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder](): F[ReProvisioningStatus[F]] = for {
    storeConfig           <- MigrationsConnectionConfig[F]()
    renkuUrl              <- RenkuUrlLoader[F]()
    subscriptionsRegistry <- Ref.of(List.empty[SubscriptionMechanism[F]])
    lastCacheCheckTimeRef <- Ref.of[F, Long](0)
  } yield {
    implicit val baseUrl: RenkuUrl = renkuUrl
    new ReProvisioningStatusImpl(storeConfig,
                                 StatusRefreshInterval,
                                 CacheRefreshInterval,
                                 subscriptionsRegistry,
                                 lastCacheCheckTimeRef
    )
  }
}
