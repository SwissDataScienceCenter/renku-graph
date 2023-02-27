package io.renku.eventlog.events.consumers.statuschange

import io.renku.events.consumers.Project
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}

package object rollbacktoawaitingdeletion {

  lazy val rollbackToAwaitingDeletionEvents = for {
    projectId   <- projectIds
    projectPath <- projectPaths
  } yield RollbackToAwaitingDeletion(Project(projectId, projectPath))
}
