package io.renku.eventlog.events.consumers.statuschange

import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._

package object totriplesgenerated {

  lazy val toTriplesGeneratedEvents = for {
    eventId        <- compoundEventIds
    projectPath    <- projectPaths
    processingTime <- eventProcessingTimes
    payload        <- zippedEventPayloads
  } yield ToTriplesGenerated(eventId, projectPath, processingTime, payload)
}
