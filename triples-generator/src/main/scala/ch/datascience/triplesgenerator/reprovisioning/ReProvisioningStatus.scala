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

import cats.Applicative
import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.events.consumers.EventConsumersRegistry
import ch.datascience.graph.Schemas.rdf
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.{Decoder, DecodingFailure}
import io.renku.jsonld.EntityId

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

trait ReProvisioningStatus[Interpretation[_]] {
  def isReProvisioning(): Interpretation[Boolean]

  def setRunning(): Interpretation[Unit]

  def clear(): Interpretation[Unit]
}

private class ReProvisioningStatusImpl(
    eventConsumersRegistry:  EventConsumersRegistry[IO],
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO],
    statusRefreshInterval:   FiniteDuration,
    cacheRefreshInterval:    FiniteDuration,
    lastCacheCheckTimeRef:   Ref[IO, Long]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with ReProvisioningStatus[IO] {

  private val applicative = Applicative[IO]
  import ReProvisioningJsonLD._
  import applicative._
  import eventConsumersRegistry._

  private val runningStatusCheckStarted = new AtomicBoolean(false)

  override def setRunning(): IO[Unit] = updateWithNoResult {
    SparqlQuery.of(
      name = "re-provisioning - status insert",
      Prefixes.of(rdf -> "rdf"),
      s"""|INSERT DATA { 
          |  <${id(renkuBaseUrl)}> rdf:type <$objectType>;
          |                        <$reProvisioningStatus> '$Running'.
          |}
          |""".stripMargin
    )
  }

  override def clear(): IO[Unit] = for {
    _ <- deleteFromDb()
    _ <- renewAllSubscriptions()
  } yield ()

  private def deleteFromDb() = updateWithNoResult {
    SparqlQuery.of(
      name = "re-provisioning - status remove",
      Prefixes.of(rdf -> "rdf"),
      s"""|DELETE { ?s ?p ?o }
          |WHERE {
          | ?s ?p ?o;
          |    rdf:type <$objectType> .
          |}
          |""".stripMargin
    )
  }

  override def isReProvisioning(): IO[Boolean] =
    for {
      isCacheExpired <- isCacheExpired
      flag <- if (isCacheExpired) fetchStatus flatMap {
                case Some(Running) => triggerPeriodicStatusCheck() map (_ => true)
                case _             => updateCacheCheckTime() map (_ => false)
              }
              else false.pure[IO]
    } yield flag

  private def isCacheExpired: IO[Boolean] =
    for {
      lastCheckTime <- lastCacheCheckTimeRef.get
      currentTime   <- timer.clock.monotonic(TimeUnit.MILLISECONDS)
    } yield currentTime - lastCheckTime > cacheRefreshInterval.toMillis

  private def updateCacheCheckTime() =
    for {
      currentTime <- timer.clock.monotonic(TimeUnit.MILLISECONDS)
      _           <- lastCacheCheckTimeRef set currentTime
    } yield ()

  private def triggerPeriodicStatusCheck(): IO[Unit] =
    whenA(!runningStatusCheckStarted.get()) {
      runningStatusCheckStarted set true
      periodicStatusCheck.start.void
    }

  private def periodicStatusCheck: IO[Unit] =
    for {
      _ <- timer sleep statusRefreshInterval
      _ <- fetchStatus flatMap {
             case Some(Running) => periodicStatusCheck
             case _ =>
               runningStatusCheckStarted set false
               renewAllSubscriptions()
           }
    } yield ()

  private def fetchStatus: IO[Option[Status]] = queryExpecting[Option[Status]] {
    SparqlQuery.of(
      name = "re-provisioning - get status",
      Prefixes.of(rdf -> "rdf"),
      s"""|SELECT ?status
          |WHERE { 
          |  <${id(renkuBaseUrl)}> rdf:type <$objectType>;
          |                        <$reProvisioningStatus> ?status.
          |}
          |""".stripMargin
    )
  }(statusDecoder)

  private lazy val statusDecoder: Decoder[Option[Status]] = {
    val ofStatuses: Decoder[Status] = _.downField("status").downField("value").as[String].flatMap {
      case Running.toString => Right(Running)
      case status           => Left(DecodingFailure(s"$status not a valid status", Nil))
    }

    _.downField("results")
      .downField("bindings")
      .as(decodeList(ofStatuses))
      .map(_.headOption)
  }
}

object ReProvisioningStatus {

  private val CacheRefreshInterval:  FiniteDuration = 2 minutes
  private val StatusRefreshInterval: FiniteDuration = 15 seconds

  def apply(
      eventConsumersRegistry: EventConsumersRegistry[IO],
      logger:                 Logger[IO],
      timeRecorder:           SparqlQueryTimeRecorder[IO],
      configuration:          Config = ConfigFactory.load()
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ReProvisioningStatus[IO]] =
    for {
      rdfStoreConfig        <- RdfStoreConfig[IO](configuration)
      renkuBaseUrl          <- RenkuBaseUrl[IO]()
      lastCacheCheckTimeRef <- Ref.of[IO, Long](0)
    } yield new ReProvisioningStatusImpl(eventConsumersRegistry,
                                         rdfStoreConfig,
                                         renkuBaseUrl,
                                         logger,
                                         timeRecorder,
                                         StatusRefreshInterval,
                                         CacheRefreshInterval,
                                         lastCacheCheckTimeRef
    )
}

private case object ReProvisioningJsonLD {
  import ch.datascience.graph.Schemas._

  def id(implicit renkuBaseUrl: RenkuBaseUrl) = EntityId.of((renkuBaseUrl / "re-provisioning").toString)
  val objectType           = renku / "ReProvisioning"
  val reProvisioningStatus = renku / "reProvisioningStatus"

  sealed trait Status
  case object Running extends Status {
    override lazy val toString: String = "running"
  }
}
