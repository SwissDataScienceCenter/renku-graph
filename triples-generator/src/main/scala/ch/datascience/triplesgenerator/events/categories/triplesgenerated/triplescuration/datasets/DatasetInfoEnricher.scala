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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration
package datasets

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.TopmostDataFinder.TopmostData
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplescuration] trait DatasetInfoEnricher[Interpretation[_]] {
  def enrichDatasetInfo(curatedTriples: CuratedTriples[Interpretation]): CurationResults[Interpretation]
}

private[triplescuration] class DatasetInfoEnricherImpl[Interpretation[_]: MonadThrow](
    datasetInfoFinder:  DatasetInfoFinder[Interpretation],
    triplesUpdater:     TriplesUpdater,
    topmostDataFinder:  TopmostDataFinder[Interpretation],
    descendantsUpdater: DescendantsUpdater
) extends DatasetInfoEnricher[Interpretation] {

  import datasetInfoFinder._
  import topmostDataFinder._
  import triplesUpdater._

  def enrichDatasetInfo(curatedTriples: CuratedTriples[Interpretation]): CurationResults[Interpretation] = for {
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
    case e @ (_: UnexpectedResponseException | _: ConnectivityException | _: ClientException) =>
      Left[ProcessingRecoverableError, List[TopmostData]](
        CurationRecoverableError("Problem with finding top most data", e)
      )
  }

  implicit class ReadabilityOps[T](value: Interpretation[T]) {
    lazy val asRightT: EitherT[Interpretation, ProcessingRecoverableError, T] =
      EitherT.right[ProcessingRecoverableError](value)
  }

}

private[triplescuration] object DatasetInfoEnricher {

  import cats.effect.Timer

  def apply(
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[DatasetInfoEnricher[IO]] =
    for {
      topmostDataFinder <- IOTopmostDataFinder(logger, timeRecorder)
    } yield new DatasetInfoEnricherImpl[IO](
      new DatasetInfoFinderImpl[IO],
      new TriplesUpdater(),
      topmostDataFinder,
      new DescendantsUpdater()
    )
}
