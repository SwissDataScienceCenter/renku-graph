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

}
