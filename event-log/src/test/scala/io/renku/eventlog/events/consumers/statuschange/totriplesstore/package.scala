package io.renku.eventlog.events.consumers.statuschange

import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._

package object totriplesstore {

  lazy val toTripleStoreEvents = for {
    eventId        <- compoundEventIds
    projectPath    <- projectPaths
    processingTime <- eventProcessingTimes
  } yield ToTriplesStore(eventId, projectPath, processingTime)
}
