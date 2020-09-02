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

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.rdfstore._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait TriplesRemover[Interpretation[_]] {
  def removeAllTriples(): Interpretation[Unit]
}

private class IOTriplesRemover(
    removalBatchSize:        Long Refined Positive,
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with TriplesRemover[IO] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder

  override def removeAllTriples(): IO[Unit] =
    queryExpecting(checkIfEmpty)(storeEmptyFlagDecoder) flatMap { isEmpty =>
      if (isEmpty) IO.unit
      else
        for {
          _ <- updateWitNoResult(removeTriplesBatch)
          _ <- removeAllTriples()
        } yield ()
    }

  private val checkIfEmpty = SparqlQuery(
    name = "triples remove - count",
    prefixes = Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
    ),
    """|SELECT ?subject
       |WHERE { ?subject ?p ?o 
       |  MINUS {?subject rdf:type renku:CliVersion}
       |}
       |LIMIT 1
       |""".stripMargin
  )

  private val removeTriplesBatch = SparqlQuery(
    name = "triples remove - delete",
    prefixes = Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
    ),
    s"""|DELETE { ?s ?p ?o }
        |WHERE { 
        |  SELECT ?s ?p ?o
        |  WHERE { ?s ?p ?o 
        |    MINUS {?s rdf:type renku:CliVersion}
        |  }
        |  LIMIT ${removalBatchSize.value}
        |}
        |""".stripMargin
  )

  private implicit val storeEmptyFlagDecoder: Decoder[Boolean] = {
    import io.circe.Decoder.decodeList

    val subject: Decoder[String] = _.downField("subject")
      .downField("value")
      .as[String]

    _.downField("results")
      .downField("bindings")
      .as[List[String]](decodeList(subject))
      .map(_.isEmpty)
  }
}

private object IOTriplesRemover {
  import ch.datascience.config.ConfigLoader._
  import eu.timepit.refined.pureconfig._

  def apply(
      rdfStoreConfig:          RdfStoreConfig,
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      config:                  Config = ConfigFactory.load()
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[TriplesRemover[IO]] =
    find[IO, Long Refined Positive]("re-provisioning-removal-batch-size", config) map { removalBatchSize =>
      new IOTriplesRemover(removalBatchSize, rdfStoreConfig, logger, timeRecorder)
    }
}
