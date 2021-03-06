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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.datasets.{IdSameAs, TopmostDerivedFrom, TopmostSameAs, UrlSameAs}
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.DataSetInfoFinder.DatasetInfo
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.TopmostDataFinder.TopmostData
import org.typelevel.log4cats.Logger
import io.renku.jsonld.EntityId

import scala.concurrent.ExecutionContext

private trait TopmostDataFinder[Interpretation[_]] {
  def findTopmostData(datasetInfo: DatasetInfo): Interpretation[TopmostData]
}

private class TopmostDataFinderImpl[Interpretation[_]](
    kgDatasetInfoFinder: KGDatasetInfoFinder[Interpretation]
)(implicit ME:           MonadError[Interpretation, Throwable])
    extends TopmostDataFinder[Interpretation] {

  def findTopmostData(datasetInfo: DatasetInfo): Interpretation[TopmostData] = datasetInfo match {
    case (entityId, None, None) =>
      TopmostData(entityId, TopmostSameAs(entityId), TopmostDerivedFrom(entityId)).pure[Interpretation]
    case (entityId, Some(sameAs: UrlSameAs), None) =>
      TopmostData(entityId, TopmostSameAs(sameAs), TopmostDerivedFrom(entityId)).pure[Interpretation]
    case (entityId, Some(sameAs: IdSameAs), None) =>
      kgDatasetInfoFinder.findTopmostSameAs(sameAs).map {
        case Some(parentTopmostSameAs) => TopmostData(entityId, parentTopmostSameAs, TopmostDerivedFrom(entityId))
        case None                      => TopmostData(entityId, TopmostSameAs(sameAs), TopmostDerivedFrom(entityId))
      }
    case (entityId, None, Some(derivedFrom)) =>
      kgDatasetInfoFinder.findTopmostDerivedFrom(derivedFrom).map {
        case Some(parentDerivedFrom) => TopmostData(entityId, TopmostSameAs(entityId), parentDerivedFrom)
        case None                    => TopmostData(entityId, TopmostSameAs(entityId), TopmostDerivedFrom(derivedFrom))
      }
    case (entityId, Some(_), Some(_)) =>
      new IllegalStateException(
        s"Dataset with $entityId found in the generated triples has both sameAs and derivedFrom"
      ).raiseError[Interpretation, TopmostData]
  }
}

private object IOTopmostDataFinder {
  def apply(logger:     Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[TopmostDataFinderImpl[IO]] =
    for {
      kgDatasetInfoFinder <- IOKGDatasetInfoFinder(logger, timeRecorder)
    } yield new TopmostDataFinderImpl[IO](kgDatasetInfoFinder)
}

private object TopmostDataFinder {

  final case class TopmostData(datasetId:          EntityId,
                               topmostSameAs:      TopmostSameAs,
                               topmostDerivedFrom: TopmostDerivedFrom
  )

}
