package io.renku.eventlog.events.categories.creation

import ch.datascience.events.consumers.ConsumersModelGenerators.projects
import ch.datascience.graph.model.EventsGenerators.{batchDates, eventBodies, eventIds}
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages}
import io.renku.eventlog.events.categories.creation.Event.{NewEvent, SkippedEvent}
import org.scalacheck.Gen

private object Generators {
  lazy val newEvents: Gen[NewEvent] = for {
    eventId   <- eventIds
    project   <- projects
    date      <- eventDates
    batchDate <- batchDates
    body      <- eventBodies
  } yield NewEvent(eventId, project, date, batchDate, body)

  lazy val skippedEvents: Gen[SkippedEvent] = for {
    eventId   <- eventIds
    project   <- projects
    date      <- eventDates
    batchDate <- batchDates
    body      <- eventBodies
    message   <- eventMessages
  } yield SkippedEvent(eventId, project, date, batchDate, body, message)

  implicit lazy val newOrSkippedEvents: Gen[Event] = Gen.oneOf(newEvents, skippedEvents)
}
