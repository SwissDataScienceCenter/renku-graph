package io.renku.eventlog.events.consumers.statuschange

import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths

package object rollbacktotriplesgenerated {

  lazy val rollbackToTriplesGeneratedEvents = for {
    eventId     <- compoundEventIds
    projectPath <- projectPaths
  } yield RollbackToTriplesGenerated(eventId, projectPath)
}
