package io.renku.eventlog.subscriptions.membersync

import ch.datascience.graph.model.projects

private final case class MemberSyncEvent(projectId: projects.Id) {
  override lazy val toString: String = projectId.toString
}
