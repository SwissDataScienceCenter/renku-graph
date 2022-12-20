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

package io.renku.graph.acceptancetests.stubs.gitlab

import GitLabStateUpdates.stateUpdateMonoid
import cats.effect._
import cats.syntax.all._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{RenkuUrl, persons, projects}
import io.renku.http.client.UserAccessToken
import io.renku.http.server.security.model.AuthUser
import io.renku.testtools.IOSpec
import org.http4s.Uri

/** Convenience syntax for test cases to update the [[GitLabApiStub]] state. */
trait GitLabStubIOSyntax { self: IOSpec =>
  val webhookUri: Uri = Uri.unsafeFromString("http://localhost:9001/webhooks/events")

  final implicit class StubOps(self: GitLabApiStub[IO]) {

    /** Resets the state to the empty state. */
    def clearState(): Unit =
      self.update(GitLabStateUpdates.clearState).unsafeRunSync()

    /** Adds an authenticated gitlab user. */
    def addAuthenticated(userId: persons.GitLabId, token: UserAccessToken): Unit =
      self.update(GitLabStateUpdates.addUser(userId, token)).unsafeRunSync()

    /** Adds an authenticated gitlab user. */
    def addAuthenticated(user1: AuthUser, users: AuthUser*): Unit = {
      val f = (user1 :: users.toList).map(u => GitLabStateUpdates.addUser(u.id, u.accessToken)).combineAll
      self.update(f).unsafeRunSync()
    }

    /** Adds a gitlab project. The project will be available, but no integration hooks are created. See
     * `setupProject`.
     */
    def addProject(project: Project): Unit =
      self.update(GitLabStateUpdates.addProject(project)).unsafeRunSync()

    /** Adds the given project and associates the given commits. It also installs the `webhook/events` webhook to
     *  integrate with the triples generator. 
     */
    def setupProject(project: Project, commits: CommitId*)(implicit renkuUrl: RenkuUrl): Unit =
      self.update(GitLabStateUpdates.setupProject(project, webhookUri, commits: _*)).unsafeRunSync()

    /** Adds the given project and removes any existing project with the same id. */
    def replaceProject(project: Project): Unit =
      addProject(project)

    /** Removes all existing commits of the project with `id` and associates the given commits. */
    def replaceCommits(id: projects.GitLabId, commits: CommitId*): Unit =
      self.update(GitLabStateUpdates.replaceCommits(id, commits)).unsafeRunSync()

    /** Removes a project, its commits and webhooks. */
    def removeProject(id: projects.GitLabId): Unit =
      self.update(GitLabStateUpdates.removeProject(id)).unsafeRunSync()

    /** Marks a project to be "broken". Some endpoints will return an internal server error for this project. */
    def markProjectBroken(id: projects.GitLabId): Unit =
      self.update(GitLabStateUpdates.markProjectBroken(id)).unsafeRunSync()

    /** Removes the project from the "broken" ones. */
    def unmarkProjectBroken(id: projects.GitLabId): Unit =
      self.update(GitLabStateUpdates.unmarkProjectBroken(id)).unsafeRunSync()

    /** Removes all webhooks for the project. */
    def removeWebhook(project: projects.GitLabId): Unit =
      self.update(GitLabStateUpdates.removeWebhooks(project)).unsafeRunSync()
  }
}
