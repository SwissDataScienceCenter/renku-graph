package ch.datascience.commiteventservice.events.categories

import ch.datascience.graph.model.events.CategoryName

package object globalcommitsync {
  val categoryName: CategoryName = CategoryName("COMMIT_SYNC")

//  private[commitsync] val logMessageCommon: GlobalCommitSyncEvent => String = {
//    case FullCommitSyncEvent(id, project, lastSynced) =>
//      s"$categoryName: id = $id, projectId = ${project.id}, projectPath = ${project.path}, lastSynced = $lastSynced"
//    case MinimalCommitSyncEvent(project) =>
//      s"$categoryName: projectId = ${project.id}, projectPath = ${project.path}"
//  }

}
