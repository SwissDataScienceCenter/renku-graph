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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.forks

import cats.effect.IO._
import cats.effect._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEvent
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.{CuratedTriples, CurationResults}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

trait ForkInfoUpdater[Interpretation[_]] {
  def updateForkInfo(
      projectPath:             projects.Path,
      givenCuratedTriples:     CuratedTriples[Interpretation]
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[Interpretation]
}

class ForkInfoUpdaterImpl(
    payloadTransformer:     PayloadTransformer[IO],
    updateFunctionsCreator: UpdatesCreator[IO]
) extends ForkInfoUpdater[IO] {

  override def updateForkInfo(
      projectPath:             projects.Path,
      curatedTriples:          CuratedTriples[IO]
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[IO] =
    payloadTransformer
      .transform(projectPath, curatedTriples.triples)
      .map { transformedTriples =>
        CuratedTriples(transformedTriples, curatedTriples.updatesGroups :+ updateFunctionsCreator.create(projectPath))
      }
}

object IOForkInfoUpdater {
  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[ForkInfoUpdater[IO]] =
    for {
      payloadTransformer     <- IOPayloadTransformer(gitLabThrottler, logger)
      updateFunctionsCreator <- IOUpdateFunctionsCreator(gitLabThrottler, logger, timeRecorder)
    } yield new ForkInfoUpdaterImpl(payloadTransformer, updateFunctionsCreator)
}
