package io.renku.eventlog.events.categories.creation

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventId, EventStatus}
import io.renku.eventlog.{CompoundId, EventDate, EventMessage}

private sealed trait Event extends CompoundId {
  def id:        EventId
  def project:   Project
  def date:      EventDate
  def batchDate: BatchDate
  def body:      EventBody
  def status:    EventStatus

  def withBatchDate(batchDate: BatchDate): Event
  lazy val compoundEventId: CompoundEventId = CompoundEventId(id, project.id)
}

private object Event {

  final case class NewEvent(
      id:        EventId,
      project:   Project,
      date:      EventDate,
      batchDate: BatchDate,
      body:      EventBody
  ) extends Event {
    val status: EventStatus = EventStatus.New

    override def withBatchDate(batchDate: BatchDate): Event = this.copy(batchDate = batchDate)

  }

  final case class SkippedEvent(
      id:        EventId,
      project:   Project,
      date:      EventDate,
      batchDate: BatchDate,
      body:      EventBody,
      message:   EventMessage
  ) extends Event {
    val status: EventStatus = EventStatus.Skipped

    override def withBatchDate(batchDate: BatchDate): Event = this.copy(batchDate = batchDate)
  }
}
