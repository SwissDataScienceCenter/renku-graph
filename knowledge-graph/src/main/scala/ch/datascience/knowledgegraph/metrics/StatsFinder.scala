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

package ch.datascience.knowledgegraph.metrics

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait StatsFinder[Interpretation[_]] {
  def entitiesCount: Interpretation[Map[KGEntityType, Long]]
}

class StatsFinderImpl(
    rdfStoreConfig: RdfStoreConfig,
    logger:         Logger[IO],
    timeRecorder:   SparqlQueryTimeRecorder[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO],
    ME:               MonadError[IO, Throwable]
) extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with StatsFinder[IO] {

  import EntityCount._

  override def entitiesCount: IO[Map[KGEntityType, Long]] =
    for {
      results <- queryExpecting[List[(KGEntityType, Long)]](using = query)
    } yield addMissingStatues(results.toMap)

  private lazy val query = SparqlQuery(
    name = "entities - counts",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
    ),
    s"""|SELECT ?type (COUNT(DISTINCT ?id) as ?count)
        |WHERE {
        |  ?id rdf:type ?type
        |  FILTER (?type IN (<http://schema.org/Dataset>, <http://schema.org/Project>, 
        |  <http://www.w3.org/ns/prov#Activity>,  <http://purl.org/wf4ever/wfprov#ProcessRun>,
        |  <http://purl.org/wf4ever/wfprov#WorkflowRun>))
        |}
        |GROUP BY ?type
        |""".stripMargin
  )

  private def addMissingStatues(stats: Map[KGEntityType, Long]): Map[KGEntityType, Long] =
    KGEntityType.all.map(entityType => entityType -> stats.getOrElse(entityType, 0L)).toMap

}

private object EntityCount {

  private[metrics] implicit val countsDecoder: Decoder[List[(KGEntityType, Long)]] = {
    val counts: Decoder[(KGEntityType, Long)] = { cursor =>
      for {
        entityType <- cursor.downField("type").downField("value").as[KGEntityType]
        count      <- cursor.downField("count").downField("value").as[Long]
      } yield entityType -> count
    }

    _.downField("results")
      .downField("bindings")
      .as(decodeList(counts))
  }
}

object IOStatsFinder {
  def apply(
      timeRecorder:   SparqlQueryTimeRecorder[IO],
      logger:         Logger[IO],
      rdfStoreConfig: IO[RdfStoreConfig] = RdfStoreConfig[IO]()
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      ME:               MonadError[IO, Throwable]
  ): IO[StatsFinder[IO]] =
    for {
      config <- rdfStoreConfig
    } yield new StatsFinderImpl(config, logger, timeRecorder)
}
