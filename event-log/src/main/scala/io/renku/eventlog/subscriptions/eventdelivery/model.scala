package io.renku.eventlog.subscriptions.eventdelivery

import io.renku.graph.model.projects
import cats.Show
import cats.syntax.all._
import io.renku.graph.model.events

sealed trait EventTypeId {
  def value: String
}

final case object DeletingProjectTypeId extends EventTypeId {
  override val value: String = "DELETING"
}

sealed trait EventDeliveryId {
  def projectId: projects.Id
}

final case class CompoundEventDeliveryId(compoundEventId: events.CompoundEventId) extends EventDeliveryId {
  def projectId: projects.Id = compoundEventId.projectId
}

object CompoundEventDeliveryId {
  implicit lazy val show: Show[CompoundEventDeliveryId] = eventId => eventId.compoundEventId.show
}

final case class DeletingProjectDeliverId(projectId: projects.Id) extends EventDeliveryId {
  def eventTypeId:            EventTypeId = DeletingProjectTypeId
  override lazy val toString: String      = s"deleting project delivery event, projectId = $projectId"
}

object DeletingProjectDeliverId {
  implicit lazy val show: Show[DeletingProjectDeliverId] =
    Show.show(id => show"projectId = ${id.projectId}")
}
