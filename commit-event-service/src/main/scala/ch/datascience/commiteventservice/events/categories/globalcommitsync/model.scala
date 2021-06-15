package ch.datascience.commiteventservice.events.categories.globalcommitsync

import cats.Show
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.LastSyncedDate

private case class GlobalCommitSyncEvent(project: Project, lastSynced: LastSyncedDate) {
  def show: Show[GlobalCommitSyncEvent] = Show.show(_ =>
    s"projectId = ${project.id}, projectPath = ${project.path}"
  )
}
