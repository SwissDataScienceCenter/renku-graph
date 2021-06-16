package io.renku.eventlog.init

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventId, EventStatus}
import io.renku.eventlog.{EventDate, EventMessage}

private object model {
  case class Event(id:           EventId,
                   project:      Project,
                   date:         EventDate,
                   batchDate:    BatchDate,
                   body:         EventBody,
                   status:       EventStatus,
                   maybeMessage: Option[EventMessage]
  ) {
    lazy val compoundEventId: CompoundEventId = CompoundEventId(id, project.id)
  }
}
