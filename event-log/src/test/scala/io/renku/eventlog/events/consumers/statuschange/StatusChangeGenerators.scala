/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange

import io.renku.eventlog.events.consumers.statuschange.projecteventstonew.ProjectEventsToNew
import io.renku.eventlog.events.consumers.statuschange.rollbacktoawaitingdeletion.RollbackToAwaitingDeletion
import io.renku.events.consumers.{ConsumersModelGenerators, Project}
import io.renku.eventlog.events.consumers.statuschange.rollbacktonew.RollbackToNew
import io.renku.eventlog.events.consumers.statuschange.rollbacktotriplesgenerated.RollbackToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange.toawaitingdeletion.ToAwaitingDeletion
import io.renku.eventlog.events.consumers.statuschange.tofailure.ToFailure
import io.renku.eventlog.events.consumers.statuschange.totriplesgenerated.ToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange.totriplesstore.ToTriplesStore
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{EventContentGenerators, EventsGenerators}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus
import org.scalacheck.Gen

import java.time.{Duration => JDuration}

object StatusChangeGenerators {

  val projectEventsToNewEvents = ConsumersModelGenerators.consumerProjects.map(ProjectEventsToNew(_))

  lazy val rollbackToAwaitingDeletionEvents = for {
    projectId   <- projectIds
    projectPath <- projectPaths
  } yield RollbackToAwaitingDeletion(Project(projectId, projectPath))

  lazy val rollbackToNewEvents = for {
    eventId     <- EventsGenerators.compoundEventIds
    projectPath <- projectPaths
  } yield RollbackToNew(eventId, projectPath)

  lazy val rollbackToTriplesGeneratedEvents = for {
    eventId     <- EventsGenerators.compoundEventIds
    projectPath <- projectPaths
  } yield RollbackToTriplesGenerated(eventId, projectPath)

  lazy val toAwaitingDeletionEvents = for {
    eventId     <- EventsGenerators.compoundEventIds
    projectPath <- projectPaths
  } yield ToAwaitingDeletion(eventId, projectPath)

  lazy val toFailureEvents = for {
    eventId        <- EventsGenerators.compoundEventIds
    projectPath    <- projectPaths
    message        <- EventContentGenerators.eventMessages
    executionDelay <- executionDelays.toGeneratorOfOptions
    event <- Gen.oneOf(
               ToFailure(eventId,
                         projectPath,
                         message,
                         EventStatus.GeneratingTriples,
                         EventStatus.GenerationRecoverableFailure,
                         executionDelay
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         EventStatus.GeneratingTriples,
                         EventStatus.GenerationNonRecoverableFailure,
                         maybeExecutionDelay = None
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         EventStatus.TransformingTriples,
                         EventStatus.TransformationRecoverableFailure,
                         executionDelay
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         EventStatus.TransformingTriples,
                         EventStatus.TransformationNonRecoverableFailure,
                         maybeExecutionDelay = None
               )
             )
  } yield event

  private def executionDelays: Gen[JDuration] = Gen.choose(0L, 10L).map(JDuration.ofSeconds)

  lazy val toTriplesGeneratedEvents = for {
    eventId        <- EventsGenerators.compoundEventIds
    projectPath    <- projectPaths
    processingTime <- EventsGenerators.eventProcessingTimes
    payload        <- EventsGenerators.zippedEventPayloads
  } yield ToTriplesGenerated(eventId, projectPath, processingTime, payload)

  lazy val toTripleStoreEvents = for {
    eventId        <- EventsGenerators.compoundEventIds
    projectPath    <- projectPaths
    processingTime <- EventsGenerators.eventProcessingTimes
  } yield ToTriplesStore(eventId, projectPath, processingTime)

  def statusChangeEvents: Gen[StatusChangeEvent] =
    Gen.oneOf(
      toTripleStoreEvents,
      toTriplesGeneratedEvents,
      toFailureEvents,
      toAwaitingDeletionEvents,
      rollbackToTriplesGeneratedEvents,
      rollbackToNewEvents,
      rollbackToAwaitingDeletionEvents,
      projectEventsToNewEvents
      // TODO add missing!
    )
}
