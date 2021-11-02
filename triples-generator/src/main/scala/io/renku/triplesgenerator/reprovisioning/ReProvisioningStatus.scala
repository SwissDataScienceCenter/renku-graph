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

import cats.Applicative
import cats.effect._
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.RenkuBaseUrl
import io.renku.graph.model.Schemas.renku
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.language.postfixOps

trait ReProvisioningStatus[F[_]] {
  def isReProvisioning(): F[Boolean]
  def setRunning(on: Controller): F[Unit]
  def clear(): F[Unit]
}

private class ReProvisioningStatusImpl[F[_]: Async: Logger](
    eventConsumersRegistry: EventConsumersRegistry[F],
    rdfStoreConfig:         RdfStoreConfig,
    timeRecorder:           SparqlQueryTimeRecorder[F],
    statusRefreshInterval:  FiniteDuration,
    cacheRefreshInterval:   FiniteDuration,
    lastCacheCheckTimeRef:  Ref[F, Long]
)(implicit renkuBaseUrl:    RenkuBaseUrl)
    extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with ReProvisioningStatus[F] {

  private val applicative = Applicative[F]

  import applicative._
  import eventConsumersRegistry._
  import io.renku.jsonld.syntax._

  private val runningStatusCheckStarted = new AtomicBoolean(false)

  override def setRunning(on: Controller): F[Unit] = upload(
    ReProvisioningInfo(ReProvisioningInfo.Status.Running, on).asJsonLD
  )

  override def clear(): F[Unit] = for {
    _ <- deleteFromDb()
    _ <- renewAllSubscriptions()
  } yield ()

  private def deleteFromDb() = updateWithNoResult {
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
  }

  override def isReProvisioning(): F[Boolean] = isCacheExpired >>= {
    case true =>
      fetchStatus >>= {
        case Some(ReProvisioningInfo.Status.Running) => triggerPeriodicStatusCheck() map (_ => true)
        case _                                       => updateCacheCheckTime() map (_ => false)
      }
    case false => false.pure[F]
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
    whenA(!runningStatusCheckStarted.get()) {
      runningStatusCheckStarted set true
      Spawn[F].start(periodicStatusCheck).void
    }

  private def periodicStatusCheck: F[Unit] = Temporal[F].delayBy(
    fetchStatus >>= {
      case Some(ReProvisioningInfo.Status.Running) => periodicStatusCheck
      case _ =>
        runningStatusCheckStarted set false
        renewAllSubscriptions()
    },
    time = statusRefreshInterval
  )

  private def fetchStatus: F[Option[ReProvisioningInfo.Status]] = queryExpecting[Option[ReProvisioningInfo.Status]] {
    SparqlQuery.of(
      name = "re-provisioning - get status",
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
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val ofStatuses: Decoder[ReProvisioningInfo.Status] =
      _.downField("status").downField("value").as[ReProvisioningInfo.Status]

    _.downField("results")
      .downField("bindings")
      .as(decodeList(ofStatuses))
      .map(_.headOption)
  }
}

object ReProvisioningStatus {

  private val CacheRefreshInterval:  FiniteDuration = 2 minutes
  private val StatusRefreshInterval: FiniteDuration = 15 seconds

  def apply[F[_]: Async: Logger](
      eventConsumersRegistry: EventConsumersRegistry[F],
      timeRecorder:           SparqlQueryTimeRecorder[F],
      configuration:          Config = ConfigFactory.load()
  ): F[ReProvisioningStatus[F]] = for {
    rdfStoreConfig        <- RdfStoreConfig[F](configuration)
    renkuBaseUrl          <- RenkuBaseUrlLoader[F]()
    lastCacheCheckTimeRef <- Ref.of[F, Long](0)
  } yield {
    implicit val baseUrl: RenkuBaseUrl = renkuBaseUrl
    new ReProvisioningStatusImpl(eventConsumersRegistry,
                                 rdfStoreConfig,
                                 timeRecorder,
                                 StatusRefreshInterval,
                                 CacheRefreshInterval,
                                 lastCacheCheckTimeRef
    )
  }
}
