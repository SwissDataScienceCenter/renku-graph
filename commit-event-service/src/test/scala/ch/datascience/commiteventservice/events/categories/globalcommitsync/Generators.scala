package ch.datascience.commiteventservice.events.categories.globalcommitsync

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.EventsGenerators.lastSyncedDates
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import org.scalacheck.Gen

private object Generators {
  lazy val globalCommitSyncEvents: Gen[GlobalCommitSyncEvent] = for {
    projectId      <- projectIds
    projectPath    <- projectPaths
    lastSyncedDate <- lastSyncedDates
  } yield GlobalCommitSyncEvent(Project(projectId, projectPath), lastSyncedDate)
}
