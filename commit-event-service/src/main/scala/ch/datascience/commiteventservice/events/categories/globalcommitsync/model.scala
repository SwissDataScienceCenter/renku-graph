package ch.datascience.commiteventservice.events.categories.globalcommitsync

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.{CommitId, LastSyncedDate}

private case class GlobalCommitSyncEvent(project: Project, lastSynced: LastSyncedDate, commits: List[CommitId]) {

  override def toString: String = s"projectId = ${project.id}, projectPath = ${project.path}, lastSynced = $lastSynced"

//  def show: Show[GlobalCommitSyncEvent] = Show.show(_ =>
//    s"projectId = ${project.id}, projectPath = ${project.path}"
//  )
}
