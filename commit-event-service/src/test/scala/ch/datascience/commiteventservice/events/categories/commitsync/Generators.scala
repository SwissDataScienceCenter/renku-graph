package ch.datascience.commiteventservice.events.categories.commitsync

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.EventsGenerators.{commitIds, lastSyncedDates}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import org.scalacheck.Gen

private object Generators {

  lazy val fullCommitSyncEvents: Gen[FullCommitSyncEvent] = for {
    commitId       <- commitIds
    projectId      <- projectIds
    projectPath    <- projectPaths
    lastSyncedDate <- lastSyncedDates
  } yield FullCommitSyncEvent(commitId, Project(projectId, projectPath), lastSyncedDate)

  lazy val minimalCommitSyncEvents: Gen[MinimalCommitSyncEvent] = for {
    projectId   <- projectIds
    projectPath <- projectPaths
  } yield MinimalCommitSyncEvent(Project(projectId, projectPath))

  lazy val commitSyncEvents: Gen[CommitSyncEvent] = Gen.oneOf(fullCommitSyncEvents, minimalCommitSyncEvents)
}
