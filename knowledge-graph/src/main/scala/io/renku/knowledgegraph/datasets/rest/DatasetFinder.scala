/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
import cats.effect.{Async, Spawn}
import cats.syntax.all._
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.datasets.{Identifier, ImageUri, Keyword}
import io.renku.knowledgegraph.datasets.model._
import io.renku.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait DatasetFinder[F[_]] {
  def findDataset(identifier: Identifier, authContext: AuthContext[Identifier]): F[Option[Dataset]]
}

private class DatasetFinderImpl[F[_]: Spawn](
    baseDetailsFinder: BaseDetailsFinder[F],
    creatorsFinder:    CreatorsFinder[F],
    partsFinder:       PartsFinder[F],
    projectsFinder:    ProjectsFinder[F]
) extends DatasetFinder[F] {

  import baseDetailsFinder._
  import creatorsFinder._
  import partsFinder._
  import projectsFinder._

  def findDataset(identifier: Identifier, authContext: AuthContext[Identifier]): F[Option[Dataset]] = for {
    usedInFiber       <- Spawn[F].start(findUsedIn(identifier, authContext))
    maybeDetailsFiber <- Spawn[F].start(findBaseDetails(identifier, authContext))
    keywordsFiber     <- Spawn[F].start(findKeywords(identifier))
    imagesFiber       <- Spawn[F].start(findImages(identifier))
    creatorsFiber     <- Spawn[F].start(findCreators(identifier))
    partsFiber        <- Spawn[F].start(findParts(identifier))
    usedIn <- usedInFiber.joinWith(MonadThrow[F].raiseError(new Exception("DatasetFinder usedIn fiber canceled")))
    maybeDetails <-
      maybeDetailsFiber.joinWith(MonadThrow[F].raiseError(new Exception("DatasetFinder maybeDetails fiber canceled")))
    keywords <- keywordsFiber.joinWith(MonadThrow[F].raiseError(new Exception("DatasetFinder keywords fiber canceled")))
    imageUrls <- imagesFiber.joinWith(MonadThrow[F].raiseError(new Exception("DatasetFinder imageUrls fiber canceled")))
    creators <- creatorsFiber.joinWith(MonadThrow[F].raiseError(new Exception("DatasetFinder creators fiber canceled")))
    parts    <- partsFiber.joinWith(MonadThrow[F].raiseError(new Exception("DatasetFinder parts fiber canceled")))
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

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DatasetFinder[F]] = for {
    config           <- RdfStoreConfig[F]()
    baseDetailFinder <- BaseDetailsFinder[F](config)
    creatorsFinder   <- CreatorsFinder[F](config)
    partsFinder      <- PartsFinder[F](config)
    projectsFinder   <- ProjectsFinder[F](config)
  } yield new DatasetFinderImpl[F](baseDetailFinder, creatorsFinder, partsFinder, projectsFinder)
}
