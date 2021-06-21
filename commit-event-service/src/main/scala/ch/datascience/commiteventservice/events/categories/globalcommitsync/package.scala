package ch.datascience.commiteventservice.events.categories

import ch.datascience.graph.model.events.CategoryName

package object globalcommitsync {
  val categoryName: CategoryName = CategoryName("GLOBAL_COMMIT_SYNC")

  private[globalcommitsync] val logMessageCommon: GlobalCommitSyncEvent => String = {
    case GlobalCommitSyncEvent(project, lastSynced) =>
      s"$categoryName: projectId = ${project.id}, projectPath = ${project.path}, lastSynced = $lastSynced"
  }

}
