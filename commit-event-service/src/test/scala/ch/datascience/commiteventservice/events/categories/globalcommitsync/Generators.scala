package ch.datascience.commiteventservice.events.categories.globalcommitsync

import ch.datascience.commiteventservice.events.categories.commitsync.FullCommitSyncEvent
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.EventsGenerators.{commitIds, lastSyncedDates}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import org.scalacheck.Gen

object Generators {
  lazy val globalCommitSyncEvents: Gen[GlobalCommitSyncEvent] = for {
    commitId       <- commitIds
    projectId      <- projectIds
    projectPath    <- projectPaths
    lastSyncedDate <- lastSyncedDates
  } yield GlobalCommitSyncEvent(Project(projectId, projectPath), lastSyncedDate)
}
