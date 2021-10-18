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

package io.renku.knowledgegraph.datasets.rest

import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.datasets.{Identifier, ImageUri, Keyword}
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import io.renku.knowledgegraph.datasets.model._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait DatasetFinder[Interpretation[_]] {
  def findDataset(identifier: Identifier): Interpretation[Option[Dataset]]
}

private class DatasetFinderImpl[Interpretation[_]: Timer: ContextShift: Concurrent](
    baseDetailsFinder:       BaseDetailsFinder[Interpretation],
    creatorsFinder:          CreatorsFinder[Interpretation],
    partsFinder:             PartsFinder[Interpretation],
    projectsFinder:          ProjectsFinder[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends DatasetFinder[Interpretation] {

  import baseDetailsFinder._
  import creatorsFinder._
  import partsFinder._
  import projectsFinder._

  def findDataset(identifier: Identifier): Interpretation[Option[Dataset]] = for {
    usedInFiber       <- Concurrent[Interpretation].start(findUsedIn(identifier))
    maybeDetailsFiber <- Concurrent[Interpretation].start(findBaseDetails(identifier))
    keywordsFiber     <- Concurrent[Interpretation].start(findKeywords(identifier))
    imagesFiber       <- Concurrent[Interpretation].start(findImages(identifier))
    creatorsFiber     <- Concurrent[Interpretation].start(findCreators(identifier))
    partsFiber        <- Concurrent[Interpretation].start(findParts(identifier))
    usedIn            <- usedInFiber.join
    maybeDetails      <- maybeDetailsFiber.join
    keywords          <- keywordsFiber.join
    imageUrls         <- imagesFiber.join
    creators          <- creatorsFiber.join
    parts             <- partsFiber.join
  } yield maybeDetails map { details =>
    details.copy(
      creators = creators,
      parts = parts,
      usedIn = usedIn,
      keywords = keywords,
      images = imageUrls
    )
  }

  private implicit class DatasetOps(dataset: Dataset) {
    def copy(creators: Set[DatasetCreator],
             parts:    List[DatasetPart],
             usedIn:   List[DatasetProject],
             keywords: List[Keyword],
             images:   List[ImageUri]
    ): Dataset = dataset match {
      case ds: NonModifiedDataset =>
        ds.copy(creators = creators, parts = parts, usedIn = usedIn, keywords = keywords, images = images)
      case ds: ModifiedDataset =>
        ds.copy(creators = creators, parts = parts, usedIn = usedIn, keywords = keywords, images = images)
    }
  }
}

private object DatasetFinder {

  def apply(
      timeRecorder:   SparqlQueryTimeRecorder[IO],
      rdfStoreConfig: IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      logger:         Logger[IO] = ApplicationLogger
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[DatasetFinder[IO]] = for {
    config <- rdfStoreConfig
  } yield new DatasetFinderImpl[IO](
    new BaseDetailsFinder(config, logger, timeRecorder),
    new CreatorsFinder(config, logger, timeRecorder),
    new PartsFinder(config, logger, timeRecorder),
    new ProjectsFinder(config, logger, timeRecorder)
  )
}
