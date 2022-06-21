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

package io.renku.eventlog.events.categories
package projectsync

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.eventlog.events.categories.globalcommitsyncrequest
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectInfoSynchronizerSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with GitLabClientTools[IO] {

  "syncProjectInfo" should {

    "fetches relevant project info from GitLab and " +
      "does nothing if both paths from event and GitLab are the same" in new TestCase {

        givenGitLabProject(by = event.projectId, returning = event.projectPath.some.asRight.pure[IO])

        synchronizer.syncProjectInfo(event).unsafeRunSync() shouldBe ()
      }

    "fetches relevant project info from GitLab and " +
      "if GitLab returns a different path " +
      "project info in the project table is changed " +
      "and a GLOBAL_COMMIT_SYNC_REQUEST event with the updated project info " +
      "and a CLEAN_UP event with the old project info " +
      "are sent" in new TestCase {

        val newPath = projectPaths.generateOne
        givenGitLabProject(by = event.projectId, returning = newPath.some.asRight.pure[IO])

        givenProjectChangeInDB(event.projectId, newPath, returning = ().pure[IO])

        givenSending(globalCommitSyncRequestEvent(event.projectId, newPath), returning = ().pure[IO])

        givenSending(cleanUpRequestEvent(event.projectPath), returning = ().pure[IO])

        synchronizer.syncProjectInfo(event).unsafeRunSync() shouldBe ()
      }

    "fetches relevant project info from GitLab and " +
      "if GitLab returns no path " +
      "only a CLEAN_UP event with the old project info is sent" in new TestCase {

        givenGitLabProject(by = event.projectId, returning = Option.empty[projects.Path].asRight.pure[IO])

        givenSending(cleanUpRequestEvent(event.projectPath), returning = ().pure[IO])

        synchronizer.syncProjectInfo(event).unsafeRunSync() shouldBe ()
      }

    "log an error if finding project info in GitLab returns Left" in new TestCase {
      givenGitLabProject(by = event.projectId, returning = UnauthorizedException.asLeft.pure[IO])

      synchronizer.syncProjectInfo(event).unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info(show"PROJECT_SYNC: $event failed: $UnauthorizedException"))
    }

    "fail if process fails" in new TestCase {
      val exception = exceptions.generateOne
      givenGitLabProject(by = event.projectId,
                         returning = exception.raiseError[IO, Either[UnauthorizedException, Option[projects.Path]]]
      )

      intercept[Exception] {
        synchronizer.syncProjectInfo(event).unsafeRunSync() shouldBe ()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val event = projectSyncEvents.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabProjectFetcher = mock[GitLabProjectFetcher[IO]]
    val dbUpdater            = mock[DBUpdater[IO]]
    val eventSender          = mock[EventSender[IO]]
    val synchronizer         = new ProjectInfoSynchronizerImpl[IO](gitLabProjectFetcher, dbUpdater, eventSender)

    def givenGitLabProject(by: projects.Id, returning: IO[Either[UnauthorizedException, Option[projects.Path]]]) =
      (gitLabProjectFetcher.fetchGitLabProject _)
        .expects(by)
        .returning(returning)

    def givenProjectChangeInDB(id: projects.Id, newPath: projects.Path, returning: IO[Unit]) =
      (dbUpdater.update _)
        .expects(id, newPath)
        .returning(returning)

    def givenSending(categoryAndRequest: (CategoryName, EventRequestContent.NoPayload), returning: IO[Unit]) =
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          categoryAndRequest._2,
          EventSender.EventContext(categoryAndRequest._1, show"$categoryName: sending ${categoryAndRequest._1} failed")
        )
        .returning(returning)
  }

  private def globalCommitSyncRequestEvent(id:   projects.Id,
                                           path: projects.Path
  ): (CategoryName, EventRequestContent.NoPayload) = {
    val category = globalcommitsyncrequest.categoryName
    val payload = json"""{
      "categoryName": ${category.show},
      "project": {
        "id":   ${id.value},
        "path": ${path.show}
      }
    }"""
    category -> EventRequestContent.NoPayload(payload)
  }

  private def cleanUpRequestEvent(path: projects.Path): (CategoryName, EventRequestContent.NoPayload) = {
    val category = cleanuprequest.categoryName
    val payload = json"""{
      "categoryName": ${category.show},
      "project": {
        "path": ${path.show}
      }
    }"""
    category -> EventRequestContent.NoPayload(payload)
  }

  private lazy val projectSyncEvents = for {
    id   <- projectIds
    path <- projectPaths
  } yield ProjectSyncEvent(id, path)
}
