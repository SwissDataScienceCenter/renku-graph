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
import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait StatsFinder[Interpretation[_]] {
  def entitiesCount: Interpretation[Map[KGEntityType, Long]]
}

class StatsFinderImpl(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext,
  contextShift:              ContextShift[IO],
  timer:                     Timer[IO],
  ME:                        MonadError[IO, Throwable])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with StatsFinder[IO] {

  override def entitiesCount: IO[Map[KGEntityType, Long]] =
    for {
      results <- queryExpecting[Set[(KGEntityType, Long)]](query)
      resultsWithDefaultCOunts = addMissingStatues(results.toMap)
    } yield resultsWithDefaultCOunts

  private lazy val query = SparqlQuery(
    name = "entities - counts",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?partName ?partLocation
        |WHERE {
        |  ?id rdf:type <http://schema.org/Dataset> ;
        |}
        |ORDER BY ASC(?partName)
        |""".stripMargin
  )

  private def addMissingStatues(stats: Map[KGEntityType, Long]): Map[KGEntityType, Long] =
    KGEntityType.all.map(counts => counts -> stats.getOrElse(counts, 0L)).toMap

}

object IOStatsFinder {
  def apply(
      rdfStoreConfig:          RdfStoreConfig,
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO],
    ME:                        MonadError[IO, Throwable]): IO[StatsFinder[IO]] = IO {
    new StatsFinderImpl(rdfStoreConfig, logger, timeRecorder)
  }
}
