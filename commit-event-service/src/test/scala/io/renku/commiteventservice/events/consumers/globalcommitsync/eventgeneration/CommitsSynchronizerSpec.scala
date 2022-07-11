/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.commiteventservice.events.consumers.common.SynchronizationSummary
import io.renku.commiteventservice.events.consumers.common.UpdateResult._
import io.renku.commiteventservice.events.consumers.globalcommitsync.GlobalCommitSyncEvent.CommitsInfo
import io.renku.commiteventservice.events.consumers.globalcommitsync.eventgeneration.gitlab.{GitLabCommitFetcher, GitLabCommitStatFetcher}
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Id
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.Implicits.projectIdToPath
import io.renku.http.client.AccessToken
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant
import scala.util.Random

class CommitsSynchronizerSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with MockFactory
    with ScalaCheckPropertyChecks {

  "synchronizeEvents" should {

    "do not traverse commits history " +
      "if commits count and the latest commit id are the same between EL and GitLab" in new TestCase {
        val event = globalCommitSyncEvents().generateOne

        givenAccessTokenFound(event.project.id)
        givenCommitStatsInGL(event.project.id, event.commits)

        commitsSynchronizer.synchronizeEvents(event).unsafeRunSync() shouldBe ()

        logger.loggedOnly(
          logSummary(event.project, SynchronizationSummary(Skipped.name -> event.commits.count.value.toInt))
        )
      }

    "skip commits existing on EL and GL, create missing on EL, delete missing on GL " +
      "- case of a single page" in new TestCase {
        val event = globalCommitSyncEvents().generateOne

        givenAccessTokenFound(event.project.id)
        givenCommitStatsInGL(event.project.id, commitsInfos.generateOne)

        val commonIds = commitIds.generateList()
        val glOnlyIds = commitIds.generateList()
        val elOnlyIds = commitIds.generateList()

        givenCommitsInGL(event.project.id, untilNow, PageResult(commonIds ::: glOnlyIds, maybeNextPage = None))
        givenCommitsInEL(event.project.path, untilNow, PageResult(commonIds ::: elOnlyIds, maybeNextPage = None))
        givenCommitsInGL(event.project.id, sinceNow, PageResult.empty)
        givenCommitsInEL(event.project.path, sinceNow, PageResult.empty)

        expectEventsToBeCreated(event.project, glOnlyIds)
        expectEventsToBeDeleted(event.project, elOnlyIds)

        commitsSynchronizer.synchronizeEvents(event).unsafeRunSync() shouldBe ()

        logger.loggedOnly(
          logSummary(
            event.project,
            SynchronizationSummary(Created.name -> glOnlyIds.size, Deleted.name -> elOnlyIds.size),
            executionTimeRecorder.elapsedTime.some
          )
        )
      }

    "skip commits existing on EL and GL, create missing on EL, delete missing on GL " +
      "- case of multiple pages" in new TestCase {
        forAll { (commonIds: List[CommitId], glOnlyIds: List[CommitId], elOnlyIds: List[CommitId]) =>
          val event = globalCommitSyncEvents().generateOne

          givenAccessTokenFound(event.project.id)
          givenCommitStatsInGL(event.project.id, commitsInfos.generateOne)

          givenCommitsInGL(event.project.id, untilNow, (commonIds ::: glOnlyIds).shuffle.toPages(ofSize = 2):   _*)
          givenCommitsInEL(event.project.path, untilNow, (commonIds ::: elOnlyIds).shuffle.toPages(ofSize = 2): _*)
          givenCommitsInGL(event.project.id, sinceNow, PageResult.empty)
          givenCommitsInEL(event.project.path, sinceNow, PageResult.empty)

          expectEventsToBeCreated(event.project, glOnlyIds)
          expectEventsToBeDeleted(event.project, elOnlyIds)

          commitsSynchronizer.synchronizeEvents(event).unsafeRunSync() shouldBe ()

          logger.loggedOnly(
            logSummary(
              event.project,
              SynchronizationSummary(Created.name -> glOnlyIds.size, Deleted.name -> elOnlyIds.size),
              executionTimeRecorder.elapsedTime.some
            )
          )
          logger.reset()
        }
      }

    "skip commits existing on EL and GL, create missing on EL, delete missing on GL " +
      "- case of events added after event arrival" in new TestCase {
        val commonIds = commitIds.generateNonEmptyList(minElements = 2).toList
        val glOnlyIds = commitIds.generateNonEmptyList().toList
        val elOnlyIds = commitIds.generateNonEmptyList().toList

        val commonIdsSinceArrival = commitIds.generateNonEmptyList().toList
        val glEventsSinceArrival  = commitIds.generateNonEmptyList().toList
        val elEventsSinceArrival  = commitIds.generateNonEmptyList().toList

        val event = globalCommitSyncEvents().generateOne

        givenAccessTokenFound(event.project.id)
        givenCommitStatsInGL(event.project.id, commitsInfos.generateOne)

        givenCommitsInGL(event.project.id, untilNow, (commonIds ::: glOnlyIds).shuffle.toPages(ofSize = 2):   _*)
        givenCommitsInEL(event.project.path, untilNow, (commonIds ::: elOnlyIds).shuffle.toPages(ofSize = 2): _*)
        givenCommitsInGL(event.project.id,
                         sinceNow,
                         (commonIdsSinceArrival ::: glEventsSinceArrival).shuffle.toPages(ofSize = 2): _*
        )
        givenCommitsInEL(event.project.path,
                         sinceNow,
                         (commonIdsSinceArrival ::: elEventsSinceArrival).shuffle.toPages(ofSize = 2): _*
        )

        expectEventsToBeCreated(event.project, glOnlyIds ++ glEventsSinceArrival)
        expectEventsToBeDeleted(event.project, elOnlyIds ++ elEventsSinceArrival)

        commitsSynchronizer.synchronizeEvents(event).unsafeRunSync() shouldBe ()

        logger.loggedOnly(
          logSummary(
            event.project,
            SynchronizationSummary(Created.name -> (glOnlyIds ::: glEventsSinceArrival).size,
                                   Deleted.name -> (elOnlyIds ::: elEventsSinceArrival).size
            ),
            executionTimeRecorder.elapsedTime.some
          )
        )
        logger.reset()
      }

    "fail if finding Access Token fails" in new TestCase {
      val event     = globalCommitSyncEvents().generateOne
      val exception = exceptions.generateOne

      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(exception.raiseError[IO, Option[AccessToken]])

      intercept[Exception] {
        commitsSynchronizer.synchronizeEvents(event).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(show"$categoryName: failed to sync commits for project ${event.project}", exception))
    }

    "delete all commits in EL if the project in GL is removed" in new TestCase {
      val event = globalCommitSyncEvents().generateOne

      givenAccessTokenFound(event.project.id)
      givenProjectDoesntExistInGL(event.project.id)

      givenCommitsInGL(event.project.id, untilNow)
      givenCommitsInGL(event.project.id, sinceNow)

      val elOnlyIds = commitIds.generateList()
      givenCommitsInEL(event.project.path, untilNow, elOnlyIds.toPages(2): _*)
      givenCommitsInEL(event.project.path, sinceNow)

      expectEventsToBeCreated(event.project, Nil)
      expectEventsToBeDeleted(event.project, elOnlyIds)

      commitsSynchronizer.synchronizeEvents(event).unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        logSummary(
          event.project,
          SynchronizationSummary(Created.name -> 0, Deleted.name -> elOnlyIds.size),
          executionTimeRecorder.elapsedTime.some
        )
      )
    }
  }

  private trait TestCase {

    val maybeAccessToken = personalAccessTokens.generateOption
    val now              = Instant.now()
    val untilNow         = DateCondition.Until(now)
    val sinceNow         = DateCondition.Since(now)

    implicit val logger:                TestLogger[IO]                = TestLogger()
    implicit val executionTimeRecorder: TestExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
    implicit val accessTokenFinder:     AccessTokenFinder[IO]         = mock[AccessTokenFinder[IO]]
    val gitLabCommitStatFetcher   = mock[GitLabCommitStatFetcher[IO]]
    val gitLabCommitFetcher       = mock[GitLabCommitFetcher[IO]]
    val eventLogCommitFetcher     = mock[ELCommitFetcher[IO]]
    val commitEventDeleter        = mock[CommitEventDeleter[IO]]
    val missingCommitEventCreator = mock[MissingCommitEventCreator[IO]]
    private val currentTime       = mockFunction[Instant]
    val commitsSynchronizer = new CommitsSynchronizerImpl[IO](gitLabCommitStatFetcher,
                                                              gitLabCommitFetcher,
                                                              eventLogCommitFetcher,
                                                              commitEventDeleter,
                                                              missingCommitEventCreator,
                                                              currentTime
    )

    currentTime.expects().returning(now).anyNumberOfTimes()

    def givenAccessTokenFound(projectId: Id) = (accessTokenFinder
      .findAccessToken(_: Id)(_: Id => String))
      .expects(projectId, projectIdToPath)
      .returning(maybeAccessToken.pure[IO])

    def givenProjectDoesntExistInGL(projectId: Id) =
      (gitLabCommitStatFetcher
        .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(None.pure[IO])

    def givenCommitStatsInGL(projectId: Id, commitsInfo: CommitsInfo) =
      (gitLabCommitStatFetcher
        .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(Some(ProjectCommitStats(Some(commitsInfo.latest), commitsInfo.count)).pure[IO])

    def givenCommitsInGL(projectId: Id, condition: DateCondition, pageResults: PageResult*) = {
      val lastPage =
        if (pageResults.isEmpty) Page.first
        else pageResults.reverse.tail.headOption.flatMap(_.maybeNextPage).getOrElse(Page(pageResults.size))

      if (pageResults.isEmpty)
        (gitLabCommitFetcher
          .fetchGitLabCommits(_: projects.Id, _: DateCondition, _: PagingRequest)(_: Option[AccessToken]))
          .expects(projectId, condition, pageRequest(Page.first), maybeAccessToken)
          .returning(PageResult.empty.pure[IO])
      else
        pageResults foreach { pageResult =>
          val previousPage = pageResult.maybeNextPage.map(p => Page(p.value - 1)).getOrElse(lastPage)
          (gitLabCommitFetcher
            .fetchGitLabCommits(_: projects.Id, _: DateCondition, _: PagingRequest)(_: Option[AccessToken]))
            .expects(projectId, condition, pageRequest(previousPage), maybeAccessToken)
            .returning(pageResult.pure[IO])
        }
    }

    def givenCommitsInEL(projectPath: projects.Path, condition: DateCondition, pageResults: PageResult*) = {
      val lastPage =
        if (pageResults.isEmpty) Page.first
        else pageResults.reverse.tail.headOption.flatMap(_.maybeNextPage).getOrElse(Page(pageResults.size))

      if (pageResults.isEmpty) {
        (eventLogCommitFetcher
          .fetchELCommits(_: projects.Path, _: DateCondition, _: PagingRequest))
          .expects(projectPath, condition, pageRequest(Page.first))
          .returning(PageResult.empty.pure[IO])
      } else
        pageResults foreach { pageResult =>
          val previousPage = pageResult.maybeNextPage.map(p => Page(p.value - 1)).getOrElse(lastPage)
          (eventLogCommitFetcher
            .fetchELCommits(_: projects.Path, _: DateCondition, _: PagingRequest))
            .expects(projectPath, condition, pageRequest(previousPage))
            .returning(pageResult.pure[IO])
        }
    }

    def expectEventsToBeDeleted(project: Project, commits: List[CommitId]) =
      (commitEventDeleter
        .deleteCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
        .expects(where { (p: Project, c: List[CommitId], at: Option[AccessToken]) =>
          (p == project) && (c.toSet == commits.toSet) && (at == maybeAccessToken)
        })
        .returning(SynchronizationSummary().updated(Deleted, commits.length).pure[IO])

    def expectEventsToBeCreated(project: Project, commits: List[CommitId]) =
      (missingCommitEventCreator
        .createCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
        .expects(where { (p: Project, c: List[CommitId], at: Option[AccessToken]) =>
          (p == project) && (c.toSet == commits.toSet) && (at == maybeAccessToken)
        })
        .returning(SynchronizationSummary().updated(Created, commits.length).pure[IO])
  }

  private def logSummary(project:          Project,
                         summary:          SynchronizationSummary,
                         maybeElapsedTime: Option[ElapsedTime] = None
  ) = Info(
    s"$categoryName: projectId = ${project.id}, projectPath = ${project.path} -> events generation result: ${summary.show}${maybeElapsedTime
        .map(t => s" in ${t}ms")
        .getOrElse("")}"
  )

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
}
