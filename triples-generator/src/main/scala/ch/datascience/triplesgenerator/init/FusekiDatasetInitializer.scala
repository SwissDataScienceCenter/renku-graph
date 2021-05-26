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

package ch.datascience.triplesgenerator.init

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.logging.ApplicationLogger
import ch.datascience.triplesgenerator.config.FusekiAdminConfig
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class FusekiDatasetInitializer[Interpretation[_]](
    fusekiAdminConfig:       FusekiAdminConfig,
    datasetExistenceChecker: DatasetExistenceChecker[Interpretation],
    datasetExistenceCreator: DatasetExistenceCreator[Interpretation],
    logger:                  Logger[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable]) {

  import datasetExistenceChecker._
  import datasetExistenceCreator._
  import fusekiAdminConfig._

  def run(): Interpretation[Unit] =
    for {
      datasetExists <- doesDatasetExists() recoverWith loggingError
      result        <- createDatasetIfNeeded(datasetExists) recoverWith loggingError
    } yield result

  private def createDatasetIfNeeded(datasetExists: Boolean): Interpretation[Unit] =
    if (datasetExists)
      logger.info(s"'$datasetName' dataset exists in Jena; No action needed.")
    else
      createDataset() flatMap { _ =>
        logger.info(s"'$datasetName' dataset created in Jena")
      }

  private def loggingError[T]: PartialFunction[Throwable, Interpretation[T]] = { case NonFatal(exception) =>
    logger.error(exception)(s"'$datasetName' dataset initialization in Jena failed")
    ME.raiseError(exception)
  }
}

object IOFusekiDatasetInitializer {
  def apply()(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[FusekiDatasetInitializer[IO]] =
    for {
      fusekiAdminConfig <- FusekiAdminConfig[IO]()
    } yield new FusekiDatasetInitializer[IO](
      fusekiAdminConfig,
      new DatasetExistenceCheckerImpl(fusekiAdminConfig, ApplicationLogger),
      new DatasetExistenceCreatorImpl(fusekiAdminConfig, ApplicationLogger),
      ApplicationLogger
    )
}
