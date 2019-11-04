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

package ch.datascience.triplesgenerator.reprovisioning.postreprovisioning

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.rdfstore.IORdfStoreClient.RdfDelete
import ch.datascience.rdfstore.RdfStoreConfig
import ch.datascience.triplesgenerator.reprovisioning.{IORdfStoreUpdater, RdfStoreUpdater}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

private[reprovisioning] class PostReProvisioning[Interpretation[_]](
    orphanProjectsRemover:   OrphanProjectsRemover[Interpretation],
    orphanPersonsRemover:    OrphanPersonsRemover[Interpretation],
    orphanMailtoNoneRemover: OrphanMailtoNoneRemover[Interpretation],
    mailtoEmailRemover:      MailtoEmailRemover[Interpretation],
    orphanAgentsRemover:     OrphanAgentsRemover[Interpretation],
    executionTimeRecorder:   ExecutionTimeRecorder[Interpretation],
    logger:                  Logger[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable]) {

  import executionTimeRecorder._

  private def steps: List[RdfStoreUpdater[Interpretation]] = List(
    orphanProjectsRemover,
    orphanPersonsRemover,
    orphanMailtoNoneRemover,
    mailtoEmailRemover,
    orphanAgentsRemover
  )

  def run: Interpretation[Unit] =
    for {
      _ <- (steps map run).sequence
      _ <- logger.info("Post re-provisioning finished")
    } yield ()

  private def run(step: RdfStoreUpdater[Interpretation]): Interpretation[Unit] =
    measureExecutionTime {
      step.run recoverWith errorMessage(step)
    } map logExecutionTime(withMessage = s"${step.description} executed")

  private def errorMessage(step: RdfStoreUpdater[Interpretation]): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) => logger.error(exception)(s"${step.description} failed")
  }
}

private[reprovisioning] object IOPostReProvisioning {

  def apply(
      rdfStoreConfig:          RdfStoreConfig,
      executionTimeRecorder:   ExecutionTimeRecorder[IO],
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): PostReProvisioning[IO] = new PostReProvisioning[IO](
    new IORdfStoreUpdater[RdfDelete](rdfStoreConfig, logger) with OrphanProjectsRemover[IO],
    new IORdfStoreUpdater[RdfDelete](rdfStoreConfig, logger) with OrphanPersonsRemover[IO],
    new IORdfStoreUpdater[RdfDelete](rdfStoreConfig, logger) with OrphanMailtoNoneRemover[IO],
    new IORdfStoreUpdater[RdfDelete](rdfStoreConfig, logger) with MailtoEmailRemover[IO],
    new IORdfStoreUpdater[RdfDelete](rdfStoreConfig, logger) with OrphanAgentsRemover[IO],
    executionTimeRecorder,
    logger
  )
}
