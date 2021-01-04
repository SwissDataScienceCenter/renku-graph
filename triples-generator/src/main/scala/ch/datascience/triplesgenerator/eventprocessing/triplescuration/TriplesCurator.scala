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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.MonadError
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.{DataSetInfoEnricher, IODataSetInfoEnricher}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks.{ForkInfoUpdater, IOForkInfoUpdater}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsUpdater
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

trait TriplesCurator[Interpretation[_]] {
  def curate(
      commit:                  CommitEvent,
      triples:                 JsonLDTriples
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[Interpretation]
}

private[eventprocessing] class TriplesCuratorImpl[Interpretation[_]](
    personDetailsUpdater: PersonDetailsUpdater[Interpretation],
    forkInfoUpdater:      ForkInfoUpdater[Interpretation],
    dataSetInfoEnricher:  DataSetInfoEnricher[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable])
    extends TriplesCurator[Interpretation] {

  import forkInfoUpdater._
  import personDetailsUpdater._

  override def curate(
      commit:                  CommitEvent,
      triples:                 JsonLDTriples
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[Interpretation] =
    for {
      triplesWithPersonDetails    <- updatePersonDetails(CuratedTriples(triples, updatesGroups = Nil), commit.project)
      triplesWithForkInfo         <- updateForkInfo(commit, triplesWithPersonDetails)
      triplesWithEnrichedDatasets <- dataSetInfoEnricher.enrichDataSetInfo(triplesWithForkInfo)
    } yield triplesWithEnrichedDatasets
}

private[eventprocessing] object IOTriplesCurator {

  import cats.effect.{ContextShift, IO, Timer}
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  final case class CurationRecoverableError(message: String, cause: Throwable)
      extends Exception(message, cause)
      with ProcessingRecoverableError

  object CurationRecoverableError {
    def apply(message: String): CurationRecoverableError = CurationRecoverableError(message, null)
  }

  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[TriplesCurator[IO]] =
    for {
      personDetailsUpdater <- PersonDetailsUpdater(gitLabThrottler, logger)
      forkInfoUpdater      <- IOForkInfoUpdater(gitLabThrottler, logger, timeRecorder)
      dataSetInfoEnricher  <- IODataSetInfoEnricher(logger, timeRecorder)
    } yield new TriplesCuratorImpl[IO](personDetailsUpdater, forkInfoUpdater, dataSetInfoEnricher)
}
