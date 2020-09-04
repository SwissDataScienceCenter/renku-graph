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

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.rdfstore._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.{Decoder, DecodingFailure}
import io.renku.jsonld.EntityId

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}

trait ReProvisioningStatus[Interpretation[_]] {
  def isReProvisioning: Interpretation[Boolean]
  def setRunning:       Interpretation[Unit]
  def clear:            Interpretation[Unit]
}

class ReProvisioningStatusImpl(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO],
    cacheRefresh:            FiniteDuration,
    flagCheck:               Ref[IO, Boolean]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with ReProvisioningStatus[IO] {

  import ReProvisioningJsonLD._

  override def setRunning: IO[Unit] = updateWitNoResult {
    SparqlQuery(
      name = "reprovisioning - status insert",
      Set(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
      ),
      s"""|INSERT DATA { 
          |  <${id(renkuBaseUrl)}> rdf:type <$objectType>;
          |                        <$reProvisioningStatus> '$Running'.
          |}
          |""".stripMargin
    )
  }

  override def clear: IO[Unit] = updateWitNoResult {
    SparqlQuery(
      name = "reprovisioning - status remove",
      Set(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
      ),
      s"""
         |DELETE { ?s ?p ?o } 
         |WHERE {
         | ?s ?p ?o;
         |    rdf:type <$objectType> .
         |}
         |""".stripMargin
    )
  }

  override def isReProvisioning: IO[Boolean] =
    for {
      checkNeeded <- flagCheck.get
      flag <- if (checkNeeded) fetchStatus flatMap {
               case Some(Running) => (flagCheck set true) map (_ => true)
               case _             => (flagCheck set false) flatMap (_ => waitAndClearRef.start) map (_ => false)
             } else IO.pure(false)
    } yield flag

  private def waitAndClearRef =
    for {
      _ <- timer sleep cacheRefresh
      _ <- flagCheck set true
    } yield ()

  private def fetchStatus: IO[Option[Status]] =
    queryExpecting[Option[Status]] {
      SparqlQuery(
        name = "reprovisioning - get status",
        Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"),
        s"""|SELECT ?status
            |WHERE { 
            |  ?id rdf:type <$objectType>;
            |      <$reProvisioningStatus> ?status.
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

  private val CacheRefresh: FiniteDuration = 2 minutes

  def apply(
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      configuration:           Config = ConfigFactory.load()
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[ReProvisioningStatus[IO]] =
    for {
      rdfStoreConfig <- RdfStoreConfig[IO](configuration)
      renkuBaseUrl   <- RenkuBaseUrl[IO]()
      flagCheck      <- Ref.of[IO, Boolean](true)
    } yield new ReProvisioningStatusImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder, CacheRefresh, flagCheck)
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
