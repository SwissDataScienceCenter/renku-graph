package io.renku.eventlog.subscriptions.unprocessed

import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import io.circe.Encoder

private final case class UnprocessedEvent(id: CompoundEventId, body: EventBody) {
  override lazy val toString: String = id.toString
}

private object UnprocessedEventEncoder extends Encoder[UnprocessedEvent] {

  import io.circe.Json
  import io.circe.literal.JsonStringContext

  override def apply(unprocessedEvent: UnprocessedEvent): Json =
    json"""{
        "id":      ${unprocessedEvent.id.id.value},
        "project": {
          "id":    ${unprocessedEvent.id.projectId.value}
        },
        "body":    ${unprocessedEvent.body.value}
      }"""

}
