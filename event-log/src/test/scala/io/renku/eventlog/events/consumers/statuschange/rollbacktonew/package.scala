package io.renku.eventlog.events.consumers.statuschange

import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths

package object rollbacktonew {

  lazy val rollbackToNewEvents = for {
    eventId     <- compoundEventIds
    projectPath <- projectPaths
  } yield RollbackToNew(eventId, projectPath)
}
