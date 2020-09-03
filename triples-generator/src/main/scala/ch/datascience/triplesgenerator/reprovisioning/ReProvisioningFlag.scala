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
import ch.datascience.rdfstore._
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningJsonLD.{CurrentlyReProvisioning, ObjectType}
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.{Decoder, DecodingFailure}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}
import scala.util.Try

trait ReProvisioningFlag[Interpretation[_]] {
  def currentlyReProvisioning: Interpretation[Boolean]
}

class ReProvisioningFlagImpl(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO],
    cacheRefresh:            FiniteDuration,
    flagCheck:               Ref[IO, Boolean]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with ReProvisioningFlag[IO] {

  override def currentlyReProvisioning: IO[Boolean] =
    for {
      checkNeeded <- flagCheck.get
      flag <- if (checkNeeded) findValueInDB flatMap {
               case true  => (flagCheck set true) map (_ => true)
               case false => (flagCheck set false) flatMap (_ => waitAndClearRef.start) map (_ => false)
             } else IO.pure(false)
    } yield flag

  private def waitAndClearRef =
    for {
      _ <- timer sleep cacheRefresh
      _ <- flagCheck set true
    } yield ()

  private def findValueInDB: IO[Boolean] =
    queryExpecting[Boolean] {
      SparqlQuery(
        name = "reprovisioning - get flag",
        Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"),
        s"""|SELECT ?flag
            |WHERE { 
            |  ?id rdf:type <$ObjectType>;
            |      <$CurrentlyReProvisioning> ?flag.
            |}
            |""".stripMargin
      )
    }(flagDecoder)

  private lazy val flagDecoder: Decoder[Boolean] = {
    val ofFlags: Decoder[Boolean] = _.downField("flag").downField("value").as[String].flatMap { string =>
      Try(string.toBoolean)
        .fold(
          exception => Left(DecodingFailure(s"Reprovisioning flag not of Boolean type: ${exception.getMessage}", Nil)),
          boolean => Right(boolean)
        )
    }

    _.downField("results")
      .downField("bindings")
      .as(decodeList(ofFlags))
      .map(_.headOption.getOrElse(false))
  }
}

object ReProvisioningFlag {

  private val CacheRefresh: FiniteDuration = 2 minutes

  def apply(
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      configuration:           Config = ConfigFactory.load()
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[ReProvisioningFlag[IO]] =
    for {
      rdfStoreConfig <- RdfStoreConfig[IO](configuration)
      flagCheck      <- Ref.of[IO, Boolean](true)
    } yield new ReProvisioningFlagImpl(rdfStoreConfig, logger, timeRecorder, CacheRefresh, flagCheck)
}
