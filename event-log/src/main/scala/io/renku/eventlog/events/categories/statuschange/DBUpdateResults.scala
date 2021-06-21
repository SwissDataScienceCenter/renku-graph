package io.renku.eventlog.events.categories.statuschange

import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.projects

private sealed trait DBUpdateResults

private object DBUpdateResults {

  final case class ForProject(projectPath: projects.Path, changedStatusCounts: Map[EventStatus, Int])
      extends DBUpdateResults
  final case object ForAllProjects extends DBUpdateResults
}
