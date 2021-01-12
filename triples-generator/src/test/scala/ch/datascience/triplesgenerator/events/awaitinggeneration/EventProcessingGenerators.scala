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

package ch.datascience.triplesgenerator.events.awaitinggeneration

import ch.datascience.generators.Generators.{exceptions, nonEmptyStrings}
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.EventId
import ch.datascience.triplesgenerator.events.awaitinggeneration.CommitEvent.{CommitEventWithParent, CommitEventWithoutParent}
import ch.datascience.triplesgenerator.events.awaitinggeneration.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.awaitinggeneration.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.awaitinggeneration.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import org.scalacheck.Gen

private object EventProcessingGenerators {

  implicit val commitEvents: Gen[CommitEvent] = for {
    commitId      <- commitIds
    project       <- projects
    maybeParentId <- Gen.option(commitIds)
  } yield maybeParentId match {
    case None           => CommitEventWithoutParent(EventId(commitId.value), project, commitId)
    case Some(parentId) => CommitEventWithParent(EventId(commitId.value), project, commitId, parentId)
  }

  implicit lazy val projects: Gen[Project] = for {
    projectId <- projectIds
    path      <- projectPaths
  } yield Project(projectId, path)

  lazy val curationRecoverableErrors: Gen[CurationRecoverableError] = for {
    message   <- nonEmptyStrings()
    exception <- exceptions
  } yield CurationRecoverableError(message, exception)

  lazy val generationRecoverableErrors: Gen[GenerationRecoverableError] = for {
    message <- nonEmptyStrings()
  } yield GenerationRecoverableError(message)

  lazy val processingRecoverableErrors: Gen[ProcessingRecoverableError] = Gen.oneOf(
    curationRecoverableErrors,
    generationRecoverableErrors
  )
}
