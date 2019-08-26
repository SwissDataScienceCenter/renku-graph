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

package ch.datascience.knowledgegraph.graphql.datasets

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.config.RenkuBaseUrl
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.knowledgegraph.graphql.datasets.model._
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.RdfStoreConfig
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait DataSetsFinder[Interpretation[_]] {
  def findDataSets(projectPath: ProjectPath): Interpretation[List[DataSet]]
}

class IODataSetsFinder(
    baseInfosFinder:         BaseInfosFinder,
    creatorsFinder:          CreatorsFinder,
    partsFinder:             PartsFinder
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends DataSetsFinder[IO] {

  import baseInfosFinder._
  import creatorsFinder._
  import partsFinder._

  override def findDataSets(projectPath: ProjectPath): IO[List[DataSet]] =
    for {
      baseInfos    <- findBaseInfos(projectPath)
      withCreators <- (baseInfos map addCreators(projectPath)).parSequence
      withParts    <- (withCreators map addParts(projectPath)).parSequence
    } yield withParts

  private def addCreators(projectPath: ProjectPath)(baseInfo: DataSet): IO[DataSet] =
    findCreators(projectPath, baseInfo.id).map { creators =>
      baseInfo.copy(published = baseInfo.published.copy(creators = creators))
    }

  private def addParts(projectPath: ProjectPath)(baseInfo: DataSet): IO[DataSet] =
    findParts(projectPath, baseInfo.id).map { parts =>
      baseInfo.copy(part = parts)
    }
}

object IODataSetsFinder {

  def apply(
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[DataSetsFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield
      new IODataSetsFinder(
        new BaseInfosFinder(config, renkuBaseUrl, logger),
        new CreatorsFinder(config, renkuBaseUrl, logger),
        new PartsFinder(config, renkuBaseUrl, logger)
      )
}
