package io.renku.graph.acceptancetests.stubs.gitlab

import cats.effect._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.persons.GitLabId
import io.renku.http.client.AccessToken
import io.renku.http.server.security.model.AuthUser
import io.renku.testtools.IOSpec
import org.http4s.Uri

/** Convenience syntax for test cases to update the [[GitLabApiStub]] state in an unsafe way. */
trait GitLabStubIOSyntax { self: IOSpec =>
  val webhookUri: Uri = Uri.unsafeFromString("http://localhost:9001/webhooks/events")

  final implicit class StubOps(self: GitLabApiStub[IO]) {

    def addAuthenticated(userId: GitLabId, token: AccessToken): Unit =
      self.update(GitLabStateUpdates.addUser(userId, token)).unsafeRunSync()

    def addAuthenticated(user: AuthUser): Unit =
      self.update(GitLabStateUpdates.addUser(user.id, user.accessToken)).unsafeRunSync()

    def addProject(project: Project): Unit =
      self.update(GitLabStateUpdates.addProject(project)).unsafeRunSync()

    def setupProject(project: Project, commit: CommitId*): Unit =
      self.update(GitLabStateUpdates.setupProject(project, webhookUri, commit: _*)).unsafeRunSync()
  }
}
