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
package datasets

import cats.MonadError
import cats.data.EitherT
import cats.implicits._

import scala.language.higherKinds

private[triplescuration] class DataSetInfoEnricher[Interpretation[_]](
    dataSetInfoFinder:  DataSetInfoFinder[Interpretation],
    triplesUpdater:     TriplesUpdater,
    topmostDataFinder:  TopmostDataFinder[Interpretation],
    descendantsUpdater: DescendantsUpdater
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import dataSetInfoFinder._
  import topmostDataFinder._
  import triplesUpdater._

  def enrichDataSetInfo(curatedTriples: CuratedTriples): CurationResults[Interpretation] =
    EitherT.right {
      for {
        datasetInfos <- findDatasetsInfo(curatedTriples.triples)
        topmostInfos <- datasetInfos.map(findTopmostData).toList.sequence
        updatedTriples = topmostInfos.foldLeft(curatedTriples) { mergeTopmostDataIntoTriples }
      } yield topmostInfos.foldLeft(updatedTriples) { descendantsUpdater.prepareUpdates }
    }
}
