package io.renku.eventlog.statuschange.commands

import ch.datascience.graph.model.events.CompoundEventId
import doobie.implicits._
import io.renku.eventlog.EventProcessingTime

trait StatusProcessingTime[Interpretation[_]] {
  self: ChangeStatusCommand[Interpretation] =>

  def upsertStatusProcessingTime(eventId: CompoundEventId, processingTime: EventProcessingTime) =
    sql"""|INSERT INTO
          |status_processing_time (event_id, project_id, status, processing_time)
          |VALUES (${eventId.id},  ${eventId.projectId}, $status, ${processingTime.value.toMillis})
          |ON CONFLICT (event_id, project_id, status)
          |DO UPDATE SET processing_time = EXCLUDED.processing_time;
          |""".stripMargin.update
}
