/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets.graphql

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.knowledgegraph.datasets._
import ch.datascience.knowledgegraph.datasets.model.DataSet
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.RdfStoreConfig
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait ProjectDataSetsFinder[Interpretation[_]] {
  def findDataSets(projectPath: ProjectPath): Interpretation[List[DataSet]]
}

class IOProjectDataSetsFinder(
    baseDetailsFinder:       BaseDetailsFinder,
    creatorsFinder:          CreatorsFinder,
    partsFinder:             PartsFinder,
    projectsFinder:          ProjectsFinder
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends ProjectDataSetsFinder[IO] {

  import baseDetailsFinder._
  import creatorsFinder._
  import partsFinder._
  import projectsFinder._

  override def findDataSets(projectPath: ProjectPath): IO[List[DataSet]] =
    for {
      baseDetails  <- findBaseDetails(projectPath)
      withCreators <- (baseDetails map addCreators).sequence
      withParts    <- (withCreators map addParts).sequence
      withProjects <- (withParts map addProjects).sequence
    } yield withProjects

  private def addCreators(baseInfo: DataSet): IO[DataSet] =
    findCreators(baseInfo.id).map { creators =>
      baseInfo.copy(published = baseInfo.published.copy(creators = creators))
    }

  private def addParts(baseInfo: DataSet): IO[DataSet] =
    findParts(baseInfo.id).map { parts =>
      baseInfo.copy(part = parts)
    }

  private def addProjects(baseInfo: DataSet): IO[DataSet] =
    findProjects(baseInfo.id).map { projects =>
      baseInfo.copy(project = projects)
    }
}

object IOProjectDataSetsFinder {

  def apply(
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[ProjectDataSetsFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield
      new IOProjectDataSetsFinder(
        new BaseDetailsFinder(config, renkuBaseUrl, logger),
        new CreatorsFinder(config, renkuBaseUrl, logger),
        new PartsFinder(config, renkuBaseUrl, logger),
        new ProjectsFinder(config, renkuBaseUrl, logger)
      )
}
