package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import cats.kernel.Semigroup
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.common.UpdateResult.{Deleted, Failed}
import ch.datascience.commiteventservice.events.categories.common.{CommitInfo, CommitToEventLog, EventStatusPatcher, UpdateResult}
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary.SummaryKey
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab.GitLabCommitFetcher
import ch.datascience.commiteventservice.events.categories.globalcommitsync.{GlobalCommitSyncEvent, _}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.{BatchDate, CommitId}
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.AccessTokenFinder._
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[globalcommitsync] trait GlobalCommitEventSynchronizer[Interpretation[_]] {
  def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit]
}
private[globalcommitsync] class GlobalCommitEventSynchronizerImpl[Interpretation[_]: MonadThrow](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    gitLabCommitFetcher:   GitLabCommitFetcher[Interpretation],
    eventStatusPatcher:    EventStatusPatcher[Interpretation],
    commitToEventLog:      CommitToEventLog[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation],
    clock:                 java.time.Clock = java.time.Clock.systemUTC()
) extends GlobalCommitEventSynchronizer[Interpretation] {

  import accessTokenFinder._
  import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary._
  import commitToEventLog._
  import eventStatusPatcher._
  import executionTimeRecorder._
  import gitLabCommitFetcher._

  override def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit] = for {
    maybeAccessToken <- findAccessToken(event.project.id)
    commitStats      <- fetchCommitStats(event.project.id)(maybeAccessToken)
    commitsInSync    <- commitsInSync(event, commitStats).pure[Interpretation]
    _ <- if (!commitsInSync) {
           syncCommitsAndLogSummary(event)(maybeAccessToken)
         } else ().pure[Interpretation]
  } yield ()

  private def commitsInSync(event: GlobalCommitSyncEvent, commitStats: ProjectCommitStats): Boolean =
    event.commits.length == commitStats.commitCount.value && event.commits.headOption == commitStats.maybeLatestCommit

  private def syncCommitsAndLogSummary(
      event:                   GlobalCommitSyncEvent
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[Unit] = measureExecutionTime(
    syncCommits(event)
  ) flatMap {
    logSummary(event.project)
  }

  private def syncCommits(
      event:                   GlobalCommitSyncEvent
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[SynchronizationSummary] = for {
    commitsInGL     <- fetchAllGitLabCommits(event.project.id)(maybeAccessToken)
    deletionSummary <- deleteExtraneousCommits(event.project, event.commits.filterNot(commitsInGL.contains(_)))
    creationSummary <- createMissingCommits(event, commitsInGL)
  } yield deletionSummary combine creationSummary

  private def deleteExtraneousCommits(project: Project, commitsToDelete: List[CommitId])(implicit
      maybeAccessToken:                        Option[AccessToken]
  ): Interpretation[SynchronizationSummary] = commitsToDelete.foldLeftM(SynchronizationSummary()) { (summary, commit) =>
    sendDeletionStatus(project.id, commit).map { _ =>
      val currentCount = summary.get(toSummaryKey(Deleted))
      summary.updated(Deleted, currentCount + 1)
    }
  }

  private def createMissingCommits(event:       GlobalCommitSyncEvent,
                                   commitsInGL: List[CommitId]
  ): Interpretation[SynchronizationSummary] =
    commitsInGL
      .filterNot(event.commits.contains(_))
      .map(getCommitInfo(_))
      .map(storeCommitsInEventLog(event.project, _, BatchDate(clock)))
      .foldLeftM(SynchronizationSummary()) { (summary, status: Interpretation[UpdateResult]) =>
        status.map { result =>
          val currentCount = summary.get(toSummaryKey(result))
          summary.updated(result, currentCount + 1)
        }
      }

  private def getCommitInfo(id: CommitId): CommitInfo = ???

  private def logSummary(
      project: Project
  ): ((ElapsedTime, SynchronizationSummary)) => Interpretation[Unit] = { case (elapsedTime, summary) =>
    logger.info(
      logMessageFor(
        project,
        s"events generation result: ${summary.getSummary()} in ${elapsedTime}ms"
      )
    )
  }

  private def logMessageFor(project: Project, message: String) =
    s"$categoryName: projectId = ${project.id}, projectPath = ${project.path} -> $message"
}

private[globalcommitsync] object GlobalCommitEventSynchronizer {
  def apply(gitLabThrottler:       Throttler[IO, GitLab],
            executionTimeRecorder: ExecutionTimeRecorder[IO],
            logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GlobalCommitEventSynchronizer[IO]] = for {
    accessTokenFinder   <- AccessTokenFinder(logger)
    gitLabCommitFetcher <- GitLabCommitFetcher(gitLabThrottler, logger)
    commitToEventLog    <- CommitToEventLog(logger)
    eventStatusPatcher  <- EventStatusPatcher(logger)
  } yield new GlobalCommitEventSynchronizerImpl(
    accessTokenFinder,
    gitLabCommitFetcher,
    eventStatusPatcher,
    commitToEventLog,
    executionTimeRecorder,
    logger
  )

  final class SynchronizationSummary(private val summary: Map[SummaryKey, Int]) {
    import SynchronizationSummary._
    def getSummary(): String =
      s"${get("Created")} created, ${get("Existed")} existed, ${get("Skipped")} skipped, ${get("Deleted")} deleted, ${get("Failed")} failed"

    def get(key: String) = summary.getOrElse(key, 0)

    def updated(result: UpdateResult, newValue: Int): SynchronizationSummary = {
      val newSummary = summary.updated(toSummaryKey(result), newValue)
      new SynchronizationSummary(newSummary)
    }
  }

  object SynchronizationSummary {

    def apply() = new SynchronizationSummary(Map.empty[SummaryKey, Int])

    implicit val semigroup: Semigroup[SynchronizationSummary] =
      (x: SynchronizationSummary, y: SynchronizationSummary) => new SynchronizationSummary(x.summary combine y.summary)

    type SummaryKey = String

    def toSummaryKey(result: UpdateResult): SummaryKey = result match {
      case Failed(_, _) => "Failed"
      case s            => s.toString
    }
  }
}
