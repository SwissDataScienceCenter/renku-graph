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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.MonadError
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.{CypherQuery, JsonLDTriples, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.CurationUpdatesGroup
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.{IONeo4jDatasetInfoEnricher, Neo4jDatasetInfoEnricher}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait GraphCurator[Interpretation[_]] {
  def curate(
      commit:                  CommitEvent,
      triples:                 JsonLDTriples
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[Interpretation, CypherQuery]
}

private[eventprocessing] class GraphCuratorImpl[Interpretation[_]](
    dataSetInfoEnricher: Neo4jDatasetInfoEnricher[Interpretation]
)(implicit ME:           MonadError[Interpretation, Throwable])
    extends GraphCurator[Interpretation] {

  override def curate(
      commit:                  CommitEvent,
      triples:                 JsonLDTriples
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[Interpretation, CypherQuery] =
    for {
      triplesWithEnrichedDatasets <-
        dataSetInfoEnricher.enrichDataSetInfo(
          CuratedTriples(triples, List.empty[CurationUpdatesGroup[Interpretation, CypherQuery]])
        )
    } yield triplesWithEnrichedDatasets
}

private[eventprocessing] object IOGraphCurator {

  import cats.effect.{ContextShift, IO, Timer}
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  final case class CurationRecoverableError(message: String, cause: Throwable)
      extends Exception(message, cause)
      with ProcessingRecoverableError

  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[GraphCurator[IO]] =
    for {
      datasetInfoEnricher <- IONeo4jDatasetInfoEnricher(logger, timeRecorder)
    } yield new GraphCuratorImpl(datasetInfoEnricher)
}
