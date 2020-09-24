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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.MonadError
import cats.data.EitherT
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.http.client.RestClientError.{ConnectivityException, UnexpectedResponseException}
import ch.datascience.rdfstore.{CypherQuery, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.{CuratedTriples, CurationResults}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private[triplescuration] trait Neo4jDatasetInfoEnricher[Interpretation[_]] {
  def enrichDataSetInfo(
      curatedTriples: CuratedTriples[Interpretation, CypherQuery]
  ): CurationResults[Interpretation, CypherQuery]
}

private[triplescuration] class Neo4jDatasetInfoEnricherImpl[Interpretation[_]](
    dataSetInfoFinder:  DataSetInfoFinder[Interpretation],
    triplesUpdater:     TriplesUpdater,
    topmostDataFinder:  TopmostDataFinder[Interpretation],
    descendantsUpdater: Neo4jDescendantsUpdater
)(implicit ME:          MonadError[Interpretation, Throwable])
    extends Neo4jDatasetInfoEnricher[Interpretation] {

  import dataSetInfoFinder._
  import topmostDataFinder._
  import triplesUpdater._

  def enrichDataSetInfo(
      curatedTriples: CuratedTriples[Interpretation, CypherQuery]
  ): CurationResults[Interpretation, CypherQuery] =
    for {
      datasetInfos <- findDatasetsInfo(curatedTriples.triples).asRightT
      topmostInfos <- EitherT(
                        datasetInfos
                          .map(findTopmostData)
                          .toList
                          .sequence
                          .map(_.asRight[ProcessingRecoverableError])
                          .recover(maybeToRecoverableError)
                      )
      updatedTriples = topmostInfos.foldLeft(curatedTriples)(mergeTopmostDataIntoTriples)
    } yield topmostInfos.foldLeft(updatedTriples)(descendantsUpdater.prepareUpdates[Interpretation])

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, Either[ProcessingRecoverableError, List[TopmostData]]] = {
    case e: UnexpectedResponseException =>
      Left[ProcessingRecoverableError, List[TopmostData]](
        CurationRecoverableError("Problem with finding top most data", e)
      )
    case e: ConnectivityException =>
      Left[ProcessingRecoverableError, List[TopmostData]](
        CurationRecoverableError("Problem with finding top most data", e)
      )
  }

  implicit class ReadabilityOps[T](value: Interpretation[T]) {
    lazy val asRightT: EitherT[Interpretation, ProcessingRecoverableError, T] =
      EitherT.right[ProcessingRecoverableError](value)
  }
}

private[triplescuration] object IONeo4jDatasetInfoEnricher {
  import cats.effect.Timer

  def apply(
      logger:       Logger[IO],
      timeRecorder: SparqlQueryTimeRecorder[IO]
  )(implicit
      executionContext: ExecutionContext,
      cs:               ContextShift[IO],
      timer:            Timer[IO]
  ): IO[Neo4jDatasetInfoEnricher[IO]] =
    for {
      topmostDataFinder <- IOTopmostDataFinder(logger, timeRecorder)
    } yield new Neo4jDatasetInfoEnricherImpl[IO](
      new DataSetInfoFinderImpl[IO](),
      new TriplesUpdater(),
      topmostDataFinder,
      new Neo4jDescendantsUpdater()
    )
}
