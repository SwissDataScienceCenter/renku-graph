/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.categories.globalcommitsync
package eventgeneration

import Generators.{commitsInfos, globalCommitSyncEvents}
import cats.syntax.all._
import io.renku.commiteventservice.events.categories.common.UpdateResult.{Created, Deleted}
import io.renku.commiteventservice.events.categories.common.{SynchronizationSummary, UpdateResult}
import io.renku.commiteventservice.events.categories.globalcommitsync.GlobalCommitSyncEvent.CommitsInfo
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab.{GitLabCommitFetcher, GitLabCommitStatFetcher}
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Id
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.projectIdToPath
import io.renku.http.client.AccessToken
import io.renku.http.rest.paging.model.Page
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.logging.TestExecutionTimeRecorder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util._

class CommitsSynchronizerSpec extends AnyWordSpec with should.Matchers with MockFactory with ScalaCheckPropertyChecks {

  "synchronizeEvents" should {

    "do not traverse commits history " +
      "if commits count and the latest commit id are the same between EL and GitLab" in new TestCase {
        val event = globalCommitSyncEvents().generateOne

        givenAccessTokenIsFound(event.project.id)
        givenCommitStatsInGL(event.project.id, event.commits)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.loggedOnly(
          logSummary(event.project,
                     SynchronizationSummary().updated(UpdateResult.Skipped, event.commits.count.value.toInt)
          )
        )
      }

    "skip commits existing on EL and GL, create missing on EL, delete missing on GL" +
      " - case of a single page" in new TestCase {
        val event = globalCommitSyncEvents().generateOne

        givenAccessTokenIsFound(event.project.id)
        givenCommitStatsInGL(event.project.id, commitsInfos.generateOne)

        val commonIds = commitIds.generateList()
        val glOnlyIds = commitIds.generateList()
        val elOnlyIds = commitIds.generateList()

        givenCommitsInGL(event.project.id, PageResult(commonIds ::: glOnlyIds, maybeNextPage = None))
        givenCommitsInEL(event.project.path, PageResult(commonIds ::: elOnlyIds, maybeNextPage = None))

        expectEventsToBeCreated(event.project, glOnlyIds)
        expectEventsToBeDeleted(event.project, elOnlyIds)

        commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

        logger.loggedOnly(
          logSummary(
            event.project,
            SynchronizationSummary()
              .updated(UpdateResult.Created, glOnlyIds.size)
              .updated(UpdateResult.Deleted, elOnlyIds.size),
            executionTimeRecorder.elapsedTime.some
          )
        )
      }

    "skip commits existing on EL and GL, create missing on EL, delete missing on GL" +
      " - case of multiple pages" in new TestCase {
        forAll { (commonIds: List[CommitId], glOnlyIds: List[CommitId], elOnlyIds: List[CommitId]) =>
          val event = globalCommitSyncEvents().generateOne

          givenAccessTokenIsFound(event.project.id)
          givenCommitStatsInGL(event.project.id, commitsInfos.generateOne)

          givenCommitsInGL(event.project.id, (commonIds ::: glOnlyIds).shuffle.toPages(ofSize = 2):   _*)
          givenCommitsInEL(event.project.path, (commonIds ::: elOnlyIds).shuffle.toPages(ofSize = 2): _*)

          expectEventsToBeCreated(event.project, glOnlyIds)
          expectEventsToBeDeleted(event.project, elOnlyIds)

          commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

          logger.loggedOnly(
            logSummary(
              event.project,
              SynchronizationSummary()
                .updated(UpdateResult.Created, glOnlyIds.size)
                .updated(UpdateResult.Deleted, elOnlyIds.size),
              executionTimeRecorder.elapsedTime.some
            )
          )
          logger.reset()
        }
      }

    "fail if finding Access Token fails" in new TestCase {
      val event     = globalCommitSyncEvents().generateOne
      val exception = exceptions.generateOne

      (accessTokenFinder
        .findAccessToken(_: Id)(_: Id => String))
        .expects(event.project.id, projectIdToPath)
        .returning(Failure(exception))

      commitsSynchronizer.synchronizeEvents(event) shouldBe Failure(exception)

      logger.loggedOnly(Error(s"$categoryName - failed to sync commits for project ${event.project}", exception))
    }

    "delete all Event Log commits if the project in GL was removed but there are commits for it in EL" in new TestCase {
      val event = globalCommitSyncEvents().generateOne

      givenAccessTokenIsFound(event.project.id)
      givenProjectDoesntExistInGL(event.project.id)

      givenCommitsInGL(event.project.id)

      val elOnlyIds = commitIds.generateList()
      givenCommitsInEL(event.project.path, elOnlyIds.toPages(2): _*)

      expectEventsToBeCreated(event.project, Nil)
      expectEventsToBeDeleted(event.project, elOnlyIds)

      commitsSynchronizer.synchronizeEvents(event) shouldBe ().pure[Try]

      logger.loggedOnly(
        logSummary(
          event.project,
          SynchronizationSummary().updated(UpdateResult.Created, 0).updated(UpdateResult.Deleted, elOnlyIds.size),
          executionTimeRecorder.elapsedTime.some
        )
      )
    }
  }

  private trait TestCase {

    val maybeAccessToken = personalAccessTokens.generateOption

    implicit val logger: TestLogger[Try] = TestLogger()
    val accessTokenFinder         = mock[AccessTokenFinder[Try]]
    val gitLabCommitStatFetcher   = mock[GitLabCommitStatFetcher[Try]]
    val gitLabCommitFetcher       = mock[GitLabCommitFetcher[Try]]
    val eventLogCommitFetcher     = mock[ELCommitFetcher[Try]]
    val commitEventDeleter        = mock[CommitEventDeleter[Try]]
    val missingCommitEventCreator = mock[MissingCommitEventCreator[Try]]
    val executionTimeRecorder     = TestExecutionTimeRecorder[Try]()
    val commitsSynchronizer = new CommitsSynchronizerImpl[Try](accessTokenFinder,
                                                               gitLabCommitStatFetcher,
                                                               gitLabCommitFetcher,
                                                               eventLogCommitFetcher,
                                                               commitEventDeleter,
                                                               missingCommitEventCreator,
                                                               executionTimeRecorder
    )

    def givenAccessTokenIsFound(projectId: Id) = (accessTokenFinder
      .findAccessToken(_: Id)(_: Id => String))
      .expects(projectId, projectIdToPath)
      .returning(maybeAccessToken.pure[Try])

    def givenProjectDoesntExistInGL(projectId: Id) =
      (gitLabCommitStatFetcher
        .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(None.pure[Try])

    def givenCommitStatsInGL(projectId: Id, commitsInfo: CommitsInfo) =
      (gitLabCommitStatFetcher
        .fetchCommitStats(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(Some(ProjectCommitStats(Some(commitsInfo.latest), commitsInfo.count)).pure[Try])

    def givenCommitsInGL(projectId: Id, pageResults: PageResult*) = {
      val lastPage =
        if (pageResults.isEmpty) Page.first
        else pageResults.reverse.tail.headOption.flatMap(_.maybeNextPage).getOrElse(Page(pageResults.size))

      if (pageResults.isEmpty) {
        (gitLabCommitFetcher
          .fetchGitLabCommits(_: projects.Id, _: Page)(_: Option[AccessToken]))
          .expects(projectId, Page.first, maybeAccessToken)
          .returning(PageResult.empty.pure[Try])
      } else
        pageResults foreach { pageResult =>
          val previousPage = pageResult.maybeNextPage.map(p => Page(p.value - 1)).getOrElse(lastPage)
          (gitLabCommitFetcher
            .fetchGitLabCommits(_: projects.Id, _: Page)(_: Option[AccessToken]))
            .expects(projectId, previousPage, maybeAccessToken)
            .returning(pageResult.pure[Try])
        }
    }

    def givenCommitsInEL(projectPath: projects.Path, pageResults: PageResult*) = {
      val lastPage =
        if (pageResults.isEmpty) Page.first
        else pageResults.reverse.tail.headOption.flatMap(_.maybeNextPage).getOrElse(Page(pageResults.size))

      if (pageResults.isEmpty) {
        (eventLogCommitFetcher
          .fetchELCommits(_: projects.Path, _: Page))
          .expects(projectPath, Page.first)
          .returning(PageResult.empty.pure[Try])
      } else
        pageResults foreach { pageResult =>
          val previousPage = pageResult.maybeNextPage.map(p => Page(p.value - 1)).getOrElse(lastPage)
          (eventLogCommitFetcher
            .fetchELCommits(_: projects.Path, _: Page))
            .expects(projectPath, previousPage)
            .returning(pageResult.pure[Try])
        }
    }

    def expectEventsToBeDeleted(project: Project, commits: List[CommitId]) =
      (commitEventDeleter
        .deleteCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
        .expects(where { (p: Project, c: List[CommitId], at: Option[AccessToken]) =>
          (p == project) && (c.toSet == commits.toSet) && (at == maybeAccessToken)
        })
        .returning(SynchronizationSummary().updated(Deleted, commits.length).pure[Try])

    def expectEventsToBeCreated(project: Project, commits: List[CommitId]) =
      (missingCommitEventCreator
        .createCommits(_: Project, _: List[CommitId])(_: Option[AccessToken]))
        .expects(where { (p: Project, c: List[CommitId], at: Option[AccessToken]) =>
          (p == project) && (c.toSet == commits.toSet) && (at == maybeAccessToken)
        })
        .returning(SynchronizationSummary().updated(Created, commits.length).pure[Try])
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
}
