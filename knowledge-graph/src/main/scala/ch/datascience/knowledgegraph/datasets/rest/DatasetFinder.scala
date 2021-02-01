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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.{Identifier, ImageUrl, Keyword}
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait DatasetFinder[Interpretation[_]] {
  def findDataset(identifier: Identifier): Interpretation[Option[Dataset]]
}

private class IODatasetFinder(
    baseDetailsFinder:       BaseDetailsFinder,
    creatorsFinder:          CreatorsFinder,
    partsFinder:             PartsFinder,
    projectsFinder:          ProjectsFinder
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends DatasetFinder[IO] {

  import baseDetailsFinder._
  import creatorsFinder._
  import partsFinder._
  import projectsFinder._

  def findDataset(identifier: Identifier): IO[Option[Dataset]] =
    for {
      maybeDetailsFiber <- findBaseDetails(identifier).start
      keywordsFiber     <- findKeywords(identifier).start
      imagesFiber       <- findImages(identifier).start
      creatorsFiber     <- findCreators(identifier).start
      partsFiber        <- findParts(identifier).start
      projectsFiber     <- findProjects(identifier).start
      maybeDetails      <- maybeDetailsFiber.join
      keywords          <- keywordsFiber.join
      imageUrls         <- imagesFiber.join
      creators          <- creatorsFiber.join
      parts             <- partsFiber.join
      projects          <- projectsFiber.join
    } yield maybeDetails map { details =>
      details.copy(
        published = details.published.copy(creators = creators),
        parts = parts,
        projects = projects,
        keywords = keywords,
        images = imageUrls
      )
    }

  private implicit class DatasetOps(dataset: Dataset) {
    def copy(published: DatasetPublishing,
             parts:     List[DatasetPart],
             projects:  List[DatasetProject],
             keywords:  List[Keyword],
             images:    List[ImageUrl]
    ): Dataset =
      dataset match {
        case ds: NonModifiedDataset =>
          ds.copy(published = published, parts = parts, projects = projects, keywords = keywords)
        case ds: ModifiedDataset =>
          ds.copy(published = published, parts = parts, projects = projects, keywords = keywords)
      }
  }
}

private object IODatasetFinder {

  def apply(
      timeRecorder:   SparqlQueryTimeRecorder[IO],
      rdfStoreConfig: IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:   IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:         Logger[IO] = ApplicationLogger
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[DatasetFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IODatasetFinder(
      new BaseDetailsFinder(config, logger, timeRecorder),
      new CreatorsFinder(config, renkuBaseUrl, logger, timeRecorder),
      new PartsFinder(config, renkuBaseUrl, logger, timeRecorder),
      new ProjectsFinder(config, logger, timeRecorder)
    )
}
