package io.renku.eventlog.events.consumers.statuschange

import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects

package object projecteventstonew {
  lazy val projectEventsToNewEvents = consumerProjects.map(ProjectEventsToNew(_))
}
