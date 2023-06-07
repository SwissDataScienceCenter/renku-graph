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

import cats.effect._
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectInfoSynchronizerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "syncProjectInfo" should {

    "fetches relevant project info from GitLab and " +
      "does nothing if both paths from event and GitLab are the same" in new TestCase {

        givenGitLabProject(by = event.projectId, returning = event.projectPath.some.asRight.pure[IO])

        synchronizer.syncProjectInfo(event).unsafeRunSync() shouldBe ()
      }

    "fetches relevant project info from GitLab and " +
      "if GitLab returns a different path " +
      "project info in the project table is changed " +
      "and a COMMIT_SYNC_REQUEST event with the updated project info " +
      "and a CLEAN_UP_REQUEST event with the old project info " +
      "are sent" in new TestCase {

        val newPath = projectPaths.generateOne

        inSequence {
          givenGitLabProject(by = event.projectId, returning = newPath.some.asRight.pure[IO])

          givenProjectRemovedFromDB(event.projectId, returning = ().pure[IO])

          givenSending(cleanUpRequestEvent(event), returning = ().pure[IO])

          givenSending(commitSyncRequestEvent(event.projectId, newPath), returning = ().pure[IO])
        }

        synchronizer.syncProjectInfo(event).unsafeRunSync() shouldBe ()
      }

    "fetches relevant project info from GitLab and " +
      "if GitLab returns no path " +
      "only a CLEAN_UP_REQUEST event with the old project info is sent" in new TestCase {

        givenGitLabProject(by = event.projectId, returning = Option.empty[projects.Path].asRight.pure[IO])

        givenSending(cleanUpRequestEvent(event), returning = ().pure[IO])

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

      intercept[Exception](synchronizer.syncProjectInfo(event).unsafeRunSync()) shouldBe exception
    }
  }

  private trait TestCase {
    val event = projectSyncEvents.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabProjectFetcher = mock[GitLabProjectFetcher[IO]]
    val projectRemover       = mock[ProjectRemover[IO]]
    val eventSender          = mock[EventSender[IO]]
    val synchronizer         = new ProjectInfoSynchronizerImpl[IO](gitLabProjectFetcher, projectRemover, eventSender)

    def givenGitLabProject(by: projects.GitLabId, returning: IO[Either[UnauthorizedException, Option[projects.Path]]]) =
      (gitLabProjectFetcher.fetchGitLabProject _)
        .expects(by)
        .returning(returning)

    def givenProjectRemovedFromDB(id: projects.GitLabId, returning: IO[Unit]) =
      (projectRemover.removeProject _)
        .expects(id)
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

  private def commitSyncRequestEvent(id:   projects.GitLabId,
                                     path: projects.Path
  ): (CategoryName, EventRequestContent.NoPayload) = {
    val category = commitsyncrequest.categoryName
    val payload = json"""{
      "categoryName": ${category.show},
      "project": {
        "id":   ${id.value},
        "path": ${path.show}
      }
    }"""
    category -> EventRequestContent.NoPayload(payload)
  }

  private def cleanUpRequestEvent(event: ProjectSyncEvent): (CategoryName, EventRequestContent.NoPayload) = {
    val category = cleanuprequest.categoryName
    val payload = json"""{
      "categoryName": ${category.show},
      "project": {
        "id":   ${event.projectId.value},
        "path": ${event.projectPath.show}
      }
    }"""
    category -> EventRequestContent.NoPayload(payload)
  }

  private lazy val projectSyncEvents = for {
    id   <- projectIds
    path <- projectPaths
  } yield ProjectSyncEvent(id, path)
}
