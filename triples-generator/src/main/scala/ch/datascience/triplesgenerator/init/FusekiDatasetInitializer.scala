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

package ch.datascience.triplesgenerator.init

import cats.MonadError
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.logging.IOLogger
import ch.datascience.triplesgenerator.config.{FusekiConfig, FusekiConfigProvider}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class FusekiDatasetInitializer[Interpretation[_]](
    fusekiConfigProvider:    FusekiConfigProvider[Interpretation],
    datasetExistenceChecker: DatasetExistenceChecker[Interpretation],
    datasetExistenceCreator: DatasetExistenceCreator[Interpretation],
    logger:                  Logger[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable]) {

  import datasetExistenceChecker._
  import datasetExistenceCreator._

  def run: Interpretation[Unit] =
    for {
      fusekiConfig  <- fusekiConfigProvider.get recoverWith loggingError
      datasetExists <- doesDatasetExists(fusekiConfig) recoverWith loggingError(fusekiConfig)
      result        <- createDatasetIfNeeded(datasetExists, fusekiConfig) recoverWith loggingError(fusekiConfig)
    } yield result

  private def createDatasetIfNeeded(datasetExists: Boolean, fusekiConfig: FusekiConfig): Interpretation[Unit] =
    if (datasetExists)
      logger.info(s"'${fusekiConfig.datasetName}' dataset exists in Jena; No action needed.")
    else
      createDataset(fusekiConfig).flatMap { _ =>
        logger.info(s"'${fusekiConfig.datasetName}' dataset created in Jena")
      }

  private def loggingError: PartialFunction[Throwable, Interpretation[FusekiConfig]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Dataset initialization in Jena failed. Cannot load the config")
      ME.raiseError(exception)
  }

  private def loggingError[T](fusekiConfig: FusekiConfig): PartialFunction[Throwable, Interpretation[T]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"'${fusekiConfig.datasetName}' dataset initialization in Jena failed")
      ME.raiseError(exception)
  }
}

class IOFusekiDatasetInitializer(
    implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO]
) extends FusekiDatasetInitializer[IO](
      new FusekiConfigProvider[IO](),
      new IODatasetExistenceChecker,
      new IODatasetExistenceCreator,
      new IOLogger
    )
