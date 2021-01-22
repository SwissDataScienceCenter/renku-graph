package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import ch.datascience.graph.model.events.{CompoundEventId, EventId}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.models.Project

case class TriplesGeneratedEvent(eventId: EventId, project: Project, triples: JsonLDTriples)
    extends Product
    with Serializable

object TriplesGeneratedEvent {
  implicit class TriplesGeneratedEventOps(tripleGeneratedEvent: TriplesGeneratedEvent) {
    lazy val compoundEventId: CompoundEventId =
      CompoundEventId(tripleGeneratedEvent.eventId, tripleGeneratedEvent.project.id)
  }

}
