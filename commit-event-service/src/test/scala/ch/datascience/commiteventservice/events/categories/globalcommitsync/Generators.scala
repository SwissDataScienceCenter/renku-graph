package ch.datascience.commiteventservice.events.categories.globalcommitsync

import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import ch.datascience.events.consumers.Project
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{commitIds, lastSyncedDates}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.{NonNegative, Positive}
import org.scalacheck.Gen

private object Generators {

  lazy val globalCommitSyncEventsNonZero: Gen[GlobalCommitSyncEvent] = globalCommitSyncEvents(
    CommitCount(positiveInts(max = 9999).generateOne.value)
  )

  def globalCommitSyncEvents(commitCount: CommitCount): Gen[GlobalCommitSyncEvent] =
    globalCommitSyncEvents(commitIdsGen =
      listOf(commitIds,
             minElements = Refined.unsafeApply(commitCount.value),
             maxElements = Refined.unsafeApply(commitCount.value)
      )
    )

  def globalCommitSyncEvents(projectIdGen: Gen[projects.Id] = projectIds,
                             commitIdsGen: Gen[List[CommitId]] = listOf(commitIds)
  ): Gen[GlobalCommitSyncEvent] = for {
    projectId      <- projectIdGen
    projectPath    <- projectPaths
    lastSyncedDate <- lastSyncedDates
    commitIds      <- commitIdsGen
  } yield GlobalCommitSyncEvent(Project(projectId, projectPath), lastSyncedDate, commitIds)

  def projectCommitStats(commitId:    CommitId): Gen[ProjectCommitStats] = projectCommitStats(Gen.const(Some(commitId)))
  def projectCommitStats(commitIdGen: Gen[Option[CommitId]] = commitIds.toGeneratorOfOptions): Gen[ProjectCommitStats] =
    for {
      maybeCommitId <- commitIdGen
      commitCount   <- nonNegativeInts(9999999)
    } yield ProjectCommitStats(maybeCommitId, CommitCount(commitCount.value))
}
