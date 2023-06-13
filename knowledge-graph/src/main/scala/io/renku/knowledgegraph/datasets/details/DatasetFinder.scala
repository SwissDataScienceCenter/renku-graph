/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.datasets
package details

import Dataset._
import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.{Async, Spawn}
import cats.syntax.all._
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.datasets.Keyword
import io.renku.graph.model.images.ImageUri
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait DatasetFinder[F[_]] {
  def findDataset(identifier: RequestedDataset, authContext: AuthContext[RequestedDataset]): F[Option[Dataset]]
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

  def findDataset(identifier: RequestedDataset, authContext: AuthContext[RequestedDataset]): F[Option[Dataset]] =
    findBaseDetails(identifier, authContext) >>= {
      case None => Option.empty[Dataset].pure[F]
      case Some(dataset) =>
        def cancelledExceptionFor(process: String) = new Exception(s"$process finding process cancelled")

        for {
          initialTagFiber <- Spawn[F].start(findInitialTag(dataset, authContext))
          usedInFiber     <- Spawn[F].start(findUsedIn(dataset, authContext))
          keywordsFiber   <- Spawn[F].start(findKeywords(dataset))
          imagesFiber     <- Spawn[F].start(findImages(dataset))
          creatorsFiber   <- Spawn[F].start(findCreators(dataset.project.datasetIdentifier, dataset.project.id))
          partsFiber      <- Spawn[F].start(findParts(dataset))
          maybeInitialTag <- initialTagFiber.joinWith(MonadThrow[F].raiseError(cancelledExceptionFor("initial tag")))
          usedIn          <- usedInFiber.joinWith(MonadThrow[F].raiseError(cancelledExceptionFor("usedIn")))
          keywords        <- keywordsFiber.joinWith(MonadThrow[F].raiseError(cancelledExceptionFor("keywords")))
          imageUrls       <- imagesFiber.joinWith(MonadThrow[F].raiseError(cancelledExceptionFor("imageUrls")))
          creators        <- creatorsFiber.joinWith(MonadThrow[F].raiseError(cancelledExceptionFor("creators")))
          parts           <- partsFiber.joinWith(MonadThrow[F].raiseError(cancelledExceptionFor("parts")))
        } yield dataset
          .copy(
            maybeInitialTag = maybeInitialTag,
            creators = creators,
            parts = parts,
            usedIn = usedIn,
            keywords = keywords,
            images = imageUrls
          )
          .some
    }

  private implicit class DatasetOps(dataset: Dataset) {
    def copy(maybeInitialTag: Option[Tag],
             creators:        NonEmptyList[DatasetCreator],
             parts:           List[DatasetPart],
             usedIn:          List[DatasetProject],
             keywords:        List[Keyword],
             images:          List[ImageUri]
    ): Dataset = dataset match {
      case ds: NonModifiedDataset =>
        ds.copy(maybeInitialTag = maybeInitialTag,
                creators = creators.toList,
                parts = parts,
                usedIn = usedIn,
                keywords = keywords,
                images = images
        )
      case ds: ModifiedDataset =>
        ds.copy(maybeInitialTag = maybeInitialTag,
                creators = creators.toList,
                parts = parts,
                usedIn = usedIn,
                keywords = keywords,
                images = images
        )
    }
  }
}

private object DatasetFinder {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DatasetFinder[F]] = for {
    storeConfig      <- ProjectsConnectionConfig[F]()
    baseDetailFinder <- BaseDetailsFinder[F](storeConfig)
    creatorsFinder   <- CreatorsFinder[F](storeConfig)
    partsFinder      <- PartsFinder[F](storeConfig)
    projectsFinder   <- ProjectsFinder[F](storeConfig)
  } yield new DatasetFinderImpl[F](baseDetailFinder, creatorsFinder, partsFinder, projectsFinder)
}
