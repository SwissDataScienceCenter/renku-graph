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

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait TriplesRemover[F[_]] {
  def removeAllTriples(): F[Unit]
}

private class TriplesRemoverImpl[F[_]: Async: Logger](
    removalBatchSize: Long Refined Positive,
    rdfStoreConfig:   RdfStoreConfig,
    timeRecorder:     SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with TriplesRemover[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._

  override def removeAllTriples(): F[Unit] =
    queryExpecting(checkIfEmpty)(storeEmptyFlagDecoder) flatMap { isEmpty =>
      if (isEmpty) MonadThrow[F].unit
      else
        for {
          _ <- updateWithNoResult(removeTriplesBatch)
          _ <- removeAllTriples()
        } yield ()
    }

  private val checkIfEmpty = SparqlQuery.of(
    name = "triples remove - count",
    Prefixes.of(renku -> "renku"),
    s"""|SELECT ?subject
        |WHERE { ?subject ?p ?o 
        |  MINUS {
        |    ?subject a ?type
        |    FILTER (?type IN (<${renkuVersionPairEntityType.show}>, <${ReProvisioningJsonLD.objectType}>)) 
        |  }
        |}
        |LIMIT 1
        |""".stripMargin
  )

  private val removeTriplesBatch = SparqlQuery.of(
    name = "triples remove - delete",
    Prefixes.of(renku -> "renku"),
    s"""|DELETE { ?s ?p ?o }
        |WHERE { 
        |  SELECT ?s ?p ?o
        |  WHERE { ?s ?p ?o 
        |    MINUS {
        |      ?s a ?type
        |      FILTER (?type IN (<${renkuVersionPairEntityType.show}>, <${ReProvisioningJsonLD.objectType}>)) 
        |    }
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

private object TriplesRemoverImpl {

  import eu.timepit.refined.pureconfig._
  import io.renku.config.ConfigLoader._

  def apply[F[_]: Async: Logger](
      rdfStoreConfig: RdfStoreConfig,
      timeRecorder:   SparqlQueryTimeRecorder[F],
      config:         Config = ConfigFactory.load()
  ): F[TriplesRemover[F]] = for {
    removalBatchSize <- find[F, Long Refined Positive]("re-provisioning-removal-batch-size", config)
  } yield new TriplesRemoverImpl(removalBatchSize, rdfStoreConfig, timeRecorder)
}
