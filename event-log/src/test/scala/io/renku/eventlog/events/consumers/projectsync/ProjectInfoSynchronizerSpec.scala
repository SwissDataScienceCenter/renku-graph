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

package io.renku.eventlog.events.consumers
package projectsync

import Generators._
import cats.effect._
import cats.syntax.all._
import io.renku.eventlog.api.events.{CleanUpRequest, GlobalCommitSyncRequest}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import io.renku.{eventlog, triplesgenerator}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectInfoSynchronizerSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with AsyncMockFactory
    with should.Matchers {

  it should "fetch relevant project info from GitLab and send SYNC_REPO_METADATA " +
    "if both slugs from event and GitLab are the same" in {

      val event = projectSyncEvents.generateOne

      givenGitLabProject(by = event.projectId, returning = event.projectSlug.some.asRight.pure[IO])

      givenSendingSyncRepoMetadata(event, returning = ().pure[IO])

      synchronizer.syncProjectInfo(event).assertNoException
    }

  it should "fetch relevant project info from GitLab and " +
    "if GitLab returns a different slug " +
    "project info in the project table is changed " +
    "and a GLOBAL_COMMIT_SYNC_REQUEST event with the updated project info " +
    "and a CLEAN_UP_REQUEST event with the old project info " +
    "are sent" in {

      val event   = projectSyncEvents.generateOne
      val newSlug = projectSlugs.generateOne

      givenGitLabProject(by = event.projectId, returning = newSlug.some.asRight.pure[IO])

      givenProjectRemovedFromDB(event.projectId, returning = ().pure[IO])

      givenSendingCleanUpRequest(event, returning = ().pure[IO])

      givenSendingGlobalCommitSyncRequest(event.projectId, newSlug, returning = ().pure[IO])

      synchronizer.syncProjectInfo(event).assertNoException
    }

  it should "fetch relevant project info from GitLab and " +
    "if GitLab returns no slug " +
    "only a GLOBAL_COMMIT_SYNC_REQUEST event with the old project info is sent" in {

      val event = projectSyncEvents.generateOne

      givenGitLabProject(by = event.projectId, returning = Option.empty[projects.Slug].asRight.pure[IO])

      givenSendingGlobalCommitSyncRequest(event.projectId, event.projectSlug, returning = ().pure[IO])

      synchronizer.syncProjectInfo(event).assertNoException
    }

  it should "log an error if finding project info in GitLab returns Left" in {

    val event = projectSyncEvents.generateOne

    givenGitLabProject(by = event.projectId, returning = UnauthorizedException.asLeft.pure[IO])

    synchronizer.syncProjectInfo(event).assertNoException >>
      logger.loggedOnly(Info(show"PROJECT_SYNC: $event failed: $UnauthorizedException")).pure[IO]
  }

  it should "fail if process fails" in {

    val event = projectSyncEvents.generateOne

    val exception = exceptions.generateOne
    givenGitLabProject(by = event.projectId,
                       returning = exception.raiseError[IO, Either[UnauthorizedException, Option[projects.Slug]]]
    )

    synchronizer.syncProjectInfo(event).assertThrowsError[Exception](_ shouldBe exception)
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val gitLabProjectFetcher = mock[GitLabProjectFetcher[IO]]
  private lazy val projectRemover       = mock[ProjectRemover[IO]]
  private lazy val elClient             = mock[eventlog.api.events.Client[IO]]
  private lazy val tgClient             = mock[triplesgenerator.api.events.Client[IO]]
  private lazy val synchronizer =
    new ProjectInfoSynchronizerImpl[IO](gitLabProjectFetcher, projectRemover, elClient, tgClient)

  private def givenGitLabProject(by:        projects.GitLabId,
                                 returning: IO[Either[UnauthorizedException, Option[projects.Slug]]]
  ) = (gitLabProjectFetcher.fetchGitLabProject _)
    .expects(by)
    .returning(returning)

  private def givenProjectRemovedFromDB(id: projects.GitLabId, returning: IO[Unit]) =
    (projectRemover.removeProject _)
      .expects(id)
      .returning(returning)

  private def givenSendingGlobalCommitSyncRequest(projectId:   projects.GitLabId,
                                                  projectSlug: projects.Slug,
                                                  returning:   IO[Unit]
  ) = (elClient
    .send(_: GlobalCommitSyncRequest))
    .expects(GlobalCommitSyncRequest(projectId, projectSlug))
    .returning(returning)

  private def givenSendingCleanUpRequest(event: ProjectSyncEvent, returning: IO[Unit]) =
    (elClient
      .send(_: CleanUpRequest))
      .expects(CleanUpRequest(event.projectId, event.projectSlug))
      .returning(returning)

  private def givenSendingSyncRepoMetadata(event: ProjectSyncEvent, returning: IO[Unit]) =
    (tgClient.send(_: SyncRepoMetadata)).expects(SyncRepoMetadata(event.projectSlug)).returning(returning)
}
