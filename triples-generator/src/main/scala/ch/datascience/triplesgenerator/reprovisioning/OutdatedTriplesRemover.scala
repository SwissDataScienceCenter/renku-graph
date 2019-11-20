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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait TriplesRemover[Interpretation[_]] {
  def removeAllTriples(): Interpretation[Unit]
}

private class IOTriplesRemover(
    rdfStoreConfig:          RdfStoreConfig,
    executionTimeRecorder:   ExecutionTimeRecorder[IO],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger)
    with TriplesRemover[IO] {

  import executionTimeRecorder._

  override def removeAllTriples(): IO[Unit] =
    measureExecutionTime {
      removeEverything()
    } map logExecutionTime(withMessage = "Removing outdated triples done")

  private def removeEverything(): IO[Unit] = updateWitNoResult {
    "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }"
  }
}
