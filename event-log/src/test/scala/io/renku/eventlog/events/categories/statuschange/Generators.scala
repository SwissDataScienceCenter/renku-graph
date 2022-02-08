/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.categories.statuschange

import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent._
import io.renku.events.consumers.ConsumersModelGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus._
import org.scalacheck.Gen
import io.renku.generators.Generators.Implicits._
import java.time.Duration
import io.renku.events.consumers.Project

private object Generators {

  lazy val toTriplesGeneratedEvents = for {
    eventId        <- compoundEventIds
    projectPath    <- projectPaths
    processingTime <- eventProcessingTimes
    payload        <- zippedEventPayloads
  } yield ToTriplesGenerated(eventId, projectPath, processingTime, payload)

  lazy val toTripleStoreEvents = for {
    eventId        <- compoundEventIds
    projectPath    <- projectPaths
    processingTime <- eventProcessingTimes
  } yield ToTriplesStore(eventId, projectPath, processingTime)

  private lazy val executionDelays: Gen[Duration] = Gen.choose(0L, 10L).map(Duration.ofSeconds)

  lazy val toFailureEvents = for {
    eventId        <- compoundEventIds
    projectPath    <- projectPaths
    message        <- eventMessages
    executionDelay <- executionDelays.toGeneratorOfOptions
    event <- Gen.oneOf(
               ToFailure(eventId,
                         projectPath,
                         message,
                         GeneratingTriples,
                         GenerationRecoverableFailure,
                         executionDelay
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         GeneratingTriples,
                         GenerationNonRecoverableFailure,
                         maybeExecutionDelay = None
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         TransformingTriples,
                         TransformationRecoverableFailure,
                         executionDelay
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         TransformingTriples,
                         TransformationNonRecoverableFailure,
                         maybeExecutionDelay = None
               )
             )
  } yield event

  lazy val rollbackToNewEvents = for {
    eventId     <- compoundEventIds
    projectPath <- projectPaths
  } yield RollbackToNew(eventId, projectPath)

  lazy val rollbackToTriplesGeneratedEvents = for {
    eventId     <- compoundEventIds
    projectPath <- projectPaths
  } yield RollbackToTriplesGenerated(eventId, projectPath)

  lazy val toAwaitingDeletionEvents = for {
    eventId     <- compoundEventIds
    projectPath <- projectPaths
  } yield ToAwaitingDeletion(eventId, projectPath)

  lazy val rollbackToAwaitingDeletionEvents = for {
    projectId   <- projectIds
    projectPath <- projectPaths
  } yield RollbackToAwaitingDeletion(Project(projectId, projectPath))

  lazy val projectEventToNewEvents = for {
    project <- consumerProjects
  } yield ProjectEventsToNew(project)
}
