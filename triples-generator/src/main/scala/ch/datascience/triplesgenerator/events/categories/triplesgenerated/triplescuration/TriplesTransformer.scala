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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration

import cats.MonadError
import ch.datascience.graph.model.events.EventId
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedEvent
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.{DataSetInfoEnricher, IODataSetInfoEnricher}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonDetailsUpdater
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects.{IOProjectInfoUpdater, ProjectInfoUpdater}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

trait TriplesTransformer[Interpretation[_]] {
  def transform(
      triplesGeneratedEvent:   TriplesGeneratedEvent
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[Interpretation]
}

private[events] class TriplesTransformerImpl[Interpretation[_]](
    personDetailsUpdater: PersonDetailsUpdater[Interpretation],
    projectInfoUpdater:   ProjectInfoUpdater[Interpretation],
    dataSetInfoEnricher:  DataSetInfoEnricher[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable])
    extends TriplesTransformer[Interpretation] {

  import personDetailsUpdater._
  import projectInfoUpdater._

  override def transform(
      triplesGeneratedEvent:   TriplesGeneratedEvent
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[Interpretation] =
    for {
      triplesWithPersonDetails <- updatePersonDetails(
                                    CuratedTriples(triplesGeneratedEvent.triples, updatesGroups = Nil),
                                    triplesGeneratedEvent.project.path,
                                    triplesGeneratedEvent.eventId
                                  )
      triplesWithForkInfo         <- updateProjectInfo(triplesGeneratedEvent, triplesWithPersonDetails)
      triplesWithEnrichedDatasets <- dataSetInfoEnricher.enrichDataSetInfo(triplesWithForkInfo)
    } yield triplesWithEnrichedDatasets
}

private[triplesgenerated] object IOTriplesCurator {

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
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[TriplesTransformer[IO]] =
    for {
      personDetailsUpdater <- PersonDetailsUpdater(gitLabThrottler, logger)
      forkInfoUpdater      <- IOProjectInfoUpdater(gitLabThrottler, logger, timeRecorder)
      dataSetInfoEnricher  <- IODataSetInfoEnricher(logger, timeRecorder)
    } yield new TriplesTransformerImpl[IO](personDetailsUpdater, forkInfoUpdater, dataSetInfoEnricher)
}
