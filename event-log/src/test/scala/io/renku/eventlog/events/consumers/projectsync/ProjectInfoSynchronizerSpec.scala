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
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProjectInfoSynchronizerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "syncProjectInfo" should {

    "fetches relevant project info from GitLab and " +
      "does nothing if both paths from event and GitLab are the same" in new TestCase {

        givenGitLabProject(by = event.projectId, returning = event.projectPath.some.asRight.pure[Try])

        synchronizer.syncProjectInfo(event) shouldBe ().pure[Try]
      }

    "fetches relevant project info from GitLab and " +
      "if GitLab returns a different path " +
      "project info in the project table is changed " +
      "and a COMMIT_SYNC_REQUEST event with the updated project info " +
      "and a CLEAN_UP_REQUEST event with the old project info " +
      "are sent" in new TestCase {

        val newPath = projectPaths.generateOne

        inSequence {
          givenGitLabProject(by = event.projectId, returning = newPath.some.asRight.pure[Try])

          givenProjectRemovedFromDB(event.projectId, returning = ().pure[Try])

          givenSending(cleanUpRequestEvent(event), returning = ().pure[Try])

          givenSending(commitSyncRequestEvent(event.projectId, newPath), returning = ().pure[Try])
        }

        synchronizer.syncProjectInfo(event) shouldBe ().pure[Try]
      }

    "fetches relevant project info from GitLab and " +
      "if GitLab returns no path " +
      "only a CLEAN_UP_REQUEST event with the old project info is sent" in new TestCase {

        givenGitLabProject(by = event.projectId, returning = Option.empty[projects.Path].asRight.pure[Try])

        givenSending(cleanUpRequestEvent(event), returning = ().pure[Try])

        synchronizer.syncProjectInfo(event) shouldBe ().pure[Try]
      }

    "log an error if finding project info in GitLab returns Left" in new TestCase {
      givenGitLabProject(by = event.projectId, returning = UnauthorizedException.asLeft.pure[Try])

      synchronizer.syncProjectInfo(event) shouldBe ().pure[Try]

      logger.loggedOnly(Info(show"PROJECT_SYNC: $event failed: $UnauthorizedException"))
    }

    "fail if process fails" in new TestCase {
      val exception = exceptions.generateOne
      givenGitLabProject(by = event.projectId,
                         returning = exception.raiseError[Try, Either[UnauthorizedException, Option[projects.Path]]]
      )

      synchronizer.syncProjectInfo(event) shouldBe exception.raiseError[Try, Unit]
    }
  }

  private trait TestCase {
    val event = projectSyncEvents.generateOne

    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val gitLabProjectFetcher = mock[GitLabProjectFetcher[Try]]
    val projectRemover       = mock[ProjectRemover[Try]]
    val eventSender          = mock[EventSender[Try]]
    val synchronizer         = new ProjectInfoSynchronizerImpl[Try](gitLabProjectFetcher, projectRemover, eventSender)

    def givenGitLabProject(by:        projects.GitLabId,
                           returning: Try[Either[UnauthorizedException, Option[projects.Path]]]
    ) =
      (gitLabProjectFetcher.fetchGitLabProject _)
        .expects(by)
        .returning(returning)

    def givenProjectRemovedFromDB(id: projects.GitLabId, returning: Try[Unit]) =
      (projectRemover.removeProject _)
        .expects(id)
        .returning(returning)

    def givenSending(categoryAndRequest: (CategoryName, EventRequestContent.NoPayload), returning: Try[Unit]) =
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
