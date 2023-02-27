package io.renku.eventlog.events.consumers.statuschange

import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths

package object toawaitingdeletion {

  lazy val toAwaitingDeletionEvents = for {
    eventId     <- compoundEventIds
    projectPath <- projectPaths
  } yield ToAwaitingDeletion(eventId, projectPath)
}
