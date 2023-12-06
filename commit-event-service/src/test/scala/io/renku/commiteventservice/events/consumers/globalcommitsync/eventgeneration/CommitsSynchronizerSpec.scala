/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.commiteventservice.events.consumers.globalcommitsync
package eventgeneration

import Generators.{commitsInfos, globalCommitSyncEvents}
import GlobalCommitSyncEvent.CommitsInfo
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eventgeneration.gitlab.{GitLabCommitFetcher, GitLabCommitStatFetcher}
import io.renku.commiteventservice.events.consumers.common.SynchronizationSummary
import io.renku.commiteventservice.events.consumers.common.UpdateResult._
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.Implicits.projectIdToPath
import io.renku.http.client.AccessToken
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.logging.TestExecutionTimeRecorder
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant
import scala.util.Random

class CommitsSynchronizerSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with ScalaCheckPropertyChecks
    with BeforeAndAfterEach {

  it should "do not traverse the commits history " +
    "if commits count and the latest commit id are the same between EL and GL" in {

      val event = globalCommitSyncEvents().generateOne

      implicit val at: AccessToken = givenAccessTokenFound(event.project.id)
      givenCommitStatsInGL(event.project.id, event.commits)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedOnlyF(
          Info(show"$categoryName: $event accepted"),
          logSummary(event.project, SynchronizationSummary(Skipped.name -> event.commits.count.value.toInt))
        )
    }

  it should "skip commits existing on EL and GL, create missing on EL, delete missing on GL " +
    "- case of a single page" in {

      val event = globalCommitSyncEvents().generateOne

      implicit val at: AccessToken = givenAccessTokenFound(event.project.id)
      givenCommitStatsInGL(event.project.id, commitsInfos.generateOne)

      val commonIds = commitIds.generateList()
      val glOnlyIds = commitIds.generateList()
      val elOnlyIds = commitIds.generateList()

      givenCommitsInGL(event.project.id, untilNow, PageResult(commonIds ::: glOnlyIds, maybeNextPage = None))
      givenCommitsInEL(event.project.slug, untilNow, PageResult(commonIds ::: elOnlyIds, maybeNextPage = None))
      givenCommitsInGL(event.project.id, sinceNow, PageResult.empty)
      givenCommitsInEL(event.project.slug, sinceNow, PageResult.empty)

      expectEventsToBeCreated(event.project, glOnlyIds)
      expectEventsToBeDeleted(event.project, elOnlyIds)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
          logSummary(
            event.project,
            SynchronizationSummary(Created.name -> glOnlyIds.size, Deleted.name -> elOnlyIds.size),
            executionTimeRecorder.elapsedTime.some
          )
        )
    }

  it should "skip commits existing on EL and GL, create missing on EL, delete missing on GL " +
    "- case of multiple pages" in {

      val commonIds: List[CommitId] = commitIds.generateList()
      val glOnlyIds: List[CommitId] = commitIds.generateList()
      val elOnlyIds: List[CommitId] = commitIds.generateList()
      val event = globalCommitSyncEvents().generateOne

      implicit val at: AccessToken = givenAccessTokenFound(event.project.id)
      givenCommitStatsInGL(event.project.id, commitsInfos.generateOne)

      givenCommitsInGL(event.project.id, untilNow, (commonIds ::: glOnlyIds).shuffle.toPages(ofSize = 2):   _*)
      givenCommitsInEL(event.project.slug, untilNow, (commonIds ::: elOnlyIds).shuffle.toPages(ofSize = 2): _*)
      givenCommitsInGL(event.project.id, sinceNow, PageResult.empty)
      givenCommitsInEL(event.project.slug, sinceNow, PageResult.empty)

      expectEventsToBeCreated(event.project, glOnlyIds)
      expectEventsToBeDeleted(event.project, elOnlyIds)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
          logSummary(
            event.project,
            SynchronizationSummary(Created.name -> glOnlyIds.size, Deleted.name -> elOnlyIds.size),
            executionTimeRecorder.elapsedTime.some
          )
        )
    }

  it should "skip commits existing on EL and GL, create missing on EL, delete missing on GL " +
    "- case of events added after event arrival" in {

      val commonIds = commitIds.generateNonEmptyList(min = 2).toList
      val glOnlyIds = commitIds.generateNonEmptyList().toList
      val elOnlyIds = commitIds.generateNonEmptyList().toList

      val commonIdsSinceArrival = commitIds.generateNonEmptyList().toList
      val glEventsSinceArrival  = commitIds.generateNonEmptyList().toList
      val elEventsSinceArrival  = commitIds.generateNonEmptyList().toList

      val event = globalCommitSyncEvents().generateOne

      implicit val at: AccessToken = givenAccessTokenFound(event.project.id)
      givenCommitStatsInGL(event.project.id, commitsInfos.generateOne)

      givenCommitsInGL(event.project.id, untilNow, (commonIds ::: glOnlyIds).shuffle.toPages(ofSize = 2):   _*)
      givenCommitsInEL(event.project.slug, untilNow, (commonIds ::: elOnlyIds).shuffle.toPages(ofSize = 2): _*)
      givenCommitsInGL(event.project.id,
                       sinceNow,
                       (commonIdsSinceArrival ::: glEventsSinceArrival).shuffle.toPages(ofSize = 2): _*
      )
      givenCommitsInEL(event.project.slug,
                       sinceNow,
                       (commonIdsSinceArrival ::: elEventsSinceArrival).shuffle.toPages(ofSize = 2): _*
      )

      expectEventsToBeCreated(event.project, glOnlyIds ++ glEventsSinceArrival)
      expectEventsToBeDeleted(event.project, elOnlyIds ++ elEventsSinceArrival)

      commitsSynchronizer.synchronizeEvents(event).assertNoException >>
        logger.loggedF(
          logSummary(
            event.project,
            SynchronizationSummary(Created.name -> (glOnlyIds ::: glEventsSinceArrival).size,
                                   Deleted.name -> (elOnlyIds ::: elEventsSinceArrival).size
            ),
            executionTimeRecorder.elapsedTime.some
          )
        )
    }

  it should "fail if finding Access Token fails" in {

    val event     = globalCommitSyncEvents().generateOne
    val exception = exceptions.generateOne

    givenAccessTokenFinding(event.project.id, returning = exception.raiseError[IO, Nothing])

    commitsSynchronizer.synchronizeEvents(event).assertThrowsError[Exception](_ shouldBe exception) >>
      logger.loggedF(Error(show"$categoryName: failed to sync commits for project ${event.project}", exception))
  }

  it should "delete all commits in EL if the project is removed from GL" in {

    val event = globalCommitSyncEvents().generateOne

    implicit val at: AccessToken = givenAccessTokenFound(event.project.id)
    givenProjectDoesntExistInGL(event.project.id)

    givenCommitsInGL(event.project.id, untilNow)
    givenCommitsInGL(event.project.id, sinceNow)

    val elOnlyIds = commitIds.generateList()
    givenCommitsInEL(event.project.slug, untilNow, elOnlyIds.toPages(2): _*)
    givenCommitsInEL(event.project.slug, sinceNow)

    expectEventsToBeCreated(event.project, Nil)
    expectEventsToBeDeleted(event.project, elOnlyIds)

    commitsSynchronizer.synchronizeEvents(event).assertNoException >>
      logger.loggedF(
        logSummary(
          event.project,
          SynchronizationSummary(Created.name -> 0, Deleted.name -> elOnlyIds.size),
          executionTimeRecorder.elapsedTime.some
        )
      )
  }

  it should "delete all commits in EL if no access token is available for the project" in {

    val event = globalCommitSyncEvents().generateOne

    givenAccessTokenFinding(event.project.id, returning = None.pure[IO])

    val elOnlyIds = commitIds.generateList()
    givenCommitsInEL(event.project.slug, untilNow, elOnlyIds.toPages(2): _*)

    expectEventsToBeDeleted(event.project, elOnlyIds)

    commitsSynchronizer.synchronizeEvents(event).assertNoException >>
      logger.loggedF(
        logSummary(
          event.project,
          SynchronizationSummary(Deleted.name -> elOnlyIds.size),
          executionTimeRecorder.elapsedTime.some
        )
      )
  }

  private val now           = Instant.now()
  private lazy val untilNow = DateCondition.Until(now)
  private lazy val sinceNow = DateCondition.Since(now)

  private implicit lazy val logger:                TestLogger[IO]                = TestLogger()
  private implicit lazy val executionTimeRecorder: TestExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
  private implicit val accessTokenFinder:          AccessTokenFinder[IO]         = mock[AccessTokenFinder[IO]]
  private val gitLabCommitStatFetcher   = mock[GitLabCommitStatFetcher[IO]]
  private val gitLabCommitFetcher       = mock[GitLabCommitFetcher[IO]]
  private val eventLogCommitFetcher     = mock[ELCommitFetcher[IO]]
  private val commitEventDeleter        = mock[CommitEventDeleter[IO]]
  private val missingCommitEventCreator = mock[MissingCommitEventCreator[IO]]
  private lazy val commitsSynchronizer = new CommitsSynchronizerImpl[IO](gitLabCommitStatFetcher,
                                                                         gitLabCommitFetcher,
                                                                         eventLogCommitFetcher,
                                                                         commitEventDeleter,
                                                                         missingCommitEventCreator,
                                                                         () => now
  )

  private def givenAccessTokenFound(projectId: GitLabId) = {
    val at = accessTokens.generateOne
    (accessTokenFinder
      .findAccessToken(_: GitLabId)(_: GitLabId => String))
      .expects(projectId, projectIdToPath)
      .returning(at.some.pure[IO])
    at
  }

  private def givenAccessTokenFinding(projectId: GitLabId, returning: IO[Option[AccessToken]]) =
    (accessTokenFinder
      .findAccessToken(_: GitLabId)(_: GitLabId => String))
      .expects(projectId, projectIdToPath)
      .returning(returning)

  private def givenProjectDoesntExistInGL(projectId: GitLabId)(implicit at: AccessToken) =
    (gitLabCommitStatFetcher
      .fetchCommitStats(_: projects.GitLabId)(_: Option[AccessToken]))
      .expects(projectId, at.some)
      .returning(None.pure[IO])

  private def givenCommitStatsInGL(projectId: GitLabId, commitsInfo: CommitsInfo)(implicit at: AccessToken) =
    (gitLabCommitStatFetcher
      .fetchCommitStats(_: projects.GitLabId)(_: Option[AccessToken]))
      .expects(projectId, at.some)
      .returning(Some(ProjectCommitStats(Some(commitsInfo.latest), commitsInfo.count)).pure[IO])

  private def givenCommitsInGL(projectId: GitLabId, condition: DateCondition, pageResults: PageResult*)(implicit
      at: AccessToken
  ) = {
    val lastPage =
      if (pageResults.isEmpty) Page.first
      else pageResults.reverse.tail.headOption.flatMap(_.maybeNextPage).getOrElse(Page(pageResults.size))

    if (pageResults.isEmpty)
      (gitLabCommitFetcher
        .fetchGitLabCommits(_: projects.GitLabId, _: DateCondition, _: PagingRequest)(_: Option[AccessToken]))
        .expects(projectId, condition, pageRequest(Page.first), at.some)
        .returning(PageResult.empty.pure[IO])
    else
      pageResults foreach { pageResult =>
        val previousPage = pageResult.maybeNextPage.map(p => Page(p.value - 1)).getOrElse(lastPage)
        (gitLabCommitFetcher
          .fetchGitLabCommits(_: projects.GitLabId, _: DateCondition, _: PagingRequest)(_: Option[AccessToken]))
          .expects(projectId, condition, pageRequest(previousPage), at.some)
          .returning(pageResult.pure[IO])
      }
  }

  private def givenCommitsInEL(projectSlug: projects.Slug, condition: DateCondition, pageResults: PageResult*) = {
    val lastPage =
      if (pageResults.isEmpty) Page.first
      else pageResults.reverse.tail.headOption.flatMap(_.maybeNextPage).getOrElse(Page(pageResults.size))

    if (pageResults.isEmpty) {
      (eventLogCommitFetcher
        .fetchELCommits(_: projects.Slug, _: DateCondition, _: PagingRequest))
        .expects(projectSlug, condition, pageRequest(Page.first))
        .returning(PageResult.empty.pure[IO])
    } else
      pageResults foreach { pageResult =>
        val previousPage = pageResult.maybeNextPage.map(p => Page(p.value - 1)).getOrElse(lastPage)
        (eventLogCommitFetcher
          .fetchELCommits(_: projects.Slug, _: DateCondition, _: PagingRequest))
          .expects(projectSlug, condition, pageRequest(previousPage))
          .returning(pageResult.pure[IO])
      }
  }

  private def expectEventsToBeDeleted(project: Project, commits: List[CommitId]) =
    (commitEventDeleter
      .deleteCommits(_: Project, _: List[CommitId]))
      .expects(where((p: Project, c: List[CommitId]) => (p == project) && (c.toSet == commits.toSet)))
      .returning(SynchronizationSummary().updated(Deleted, commits.length).pure[IO])

  private def expectEventsToBeCreated(project: Project, commits: List[CommitId])(implicit at: AccessToken) =
    (missingCommitEventCreator
      .createCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
      .expects(where { (p: Project, c: List[CommitId], mat: Option[AccessToken]) =>
        (p == project) && (c.toSet == commits.toSet) && (mat == at.some)
      })
      .returning(SynchronizationSummary().updated(Created, commits.length).pure[IO])

  private def logSummary(project:          Project,
                         summary:          SynchronizationSummary,
                         maybeElapsedTime: Option[ElapsedTime] = None
  ) = Info {
    val et = maybeElapsedTime.map(t => s" in ${t}ms").getOrElse("")
    show"$categoryName: $project -> events generation result: $summary$et"
  }

  private implicit class PageOps(commits: List[CommitId]) {

    lazy val shuffle: List[CommitId] = Random.shuffle(commits)

    def toPages(ofSize: Int): Seq[PageResult] = {
      val cut = commits.sliding(ofSize, step = ofSize).toList
      cut.zipWithIndex.map {
        case (list, idx) if (idx + 1) == cut.size => PageResult(list, maybeNextPage = None)
        case (list, idx)                          => PageResult(list, maybeNextPage = Page(idx + 2).some)
      }
    }
  }

  private lazy val pageRequest: Page => PagingRequest = PagingRequest(_, PerPage(50))

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    logger.reset()
  }
}
