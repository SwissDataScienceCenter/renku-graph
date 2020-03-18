/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.MonadError
import cats.data.EitherT
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.Commit
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks.{ForkInfoUpdater, IOForkInfoUpdater}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class TriplesCurator[Interpretation[_]](
    personDetailsUpdater: PersonDetailsUpdater[Interpretation],
    forkInfoUpdater:      ForkInfoUpdater[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable]) {

  import forkInfoUpdater._

  def curate(
      commit:  Commit,
      triples: JsonLDTriples
  )(
      implicit maybeAccessToken: Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, CuratedTriples] =
    for {
      triplesWithPersonDetails <- personDetailsUpdater.curate(CuratedTriples(triples, updates = Nil)).toRight
      triplesWithForkInfo      <- updateForkInfo(commit, triplesWithPersonDetails)
    } yield triplesWithForkInfo

  private implicit class InterpretationOps(out: Interpretation[CuratedTriples]) {
    lazy val toRight: EitherT[Interpretation, CurationRecoverableError, CuratedTriples] =
      EitherT.right[CurationRecoverableError](out)
  }
}

object IOTriplesCurator {

  import cats.effect.{ContextShift, IO, Timer}
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  final case class CurationRecoverableError(message: String, cause: Throwable)
      extends Exception(message, cause)
      with ProcessingRecoverableError

  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[TriplesCurator[IO]] =
    for {
      forkInfoUpdater <- IOForkInfoUpdater(gitLabThrottler, logger, timeRecorder)
    } yield new TriplesCurator[IO](
      new PersonDetailsUpdater[IO](),
      forkInfoUpdater
    )
}
