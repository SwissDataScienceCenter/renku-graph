package io.renku.eventlog.init

import ch.datascience.events.consumers.ConsumersModelGenerators.projects
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.graph.model.EventsGenerators.{batchDates, eventBodies, eventIds, eventStatuses}
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages}
import model.Event
import org.scalacheck.Gen

private object Generators {
  lazy val events: Gen[Event] = for {
    eventId      <- eventIds
    project      <- projects
    date         <- eventDates
    batchDate    <- batchDates
    body         <- eventBodies
    status       <- eventStatuses
    maybeMessage <- eventMessages.toGeneratorOfOptions
  } yield Event(eventId, project, date, batchDate, body, status, maybeMessage)
}
