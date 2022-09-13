package io.renku.graph.acceptancetests.stubs.gitlab

import cats.effect._
import cats.syntax.all._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.persons.GitLabId
import io.renku.http.client.AccessToken
import io.renku.http.server.security.model.AuthUser
import io.renku.testtools.IOSpec
import org.http4s.Uri
import GitLabStateUpdates.stateUpdateMonoid
import io.renku.graph.model.projects.Id
import io.renku.graph.model.testentities.RenkuProject

/** Convenience syntax for test cases to update the [[GitLabApiStub]] state in an unsafe way. */
trait GitLabStubIOSyntax { self: IOSpec =>
  val webhookUri: Uri = Uri.unsafeFromString("http://localhost:9001/webhooks/events")

  final implicit class StubOps(self: GitLabApiStub[IO]) {
    def clearState(): Unit =
      self.update(GitLabStateUpdates.clearState).unsafeRunSync()

    def addAuthenticated(userId: GitLabId, token: AccessToken): Unit =
      self.update(GitLabStateUpdates.addUser(userId, token)).unsafeRunSync()

    def addAuthenticated(user1: AuthUser, users: AuthUser*): Unit = {
      val f = (user1 :: users.toList).map(u => GitLabStateUpdates.addUser(u.id, u.accessToken)).combineAll
      self.update(f).unsafeRunSync()
    }

    def addProject(project: Project): Unit =
      self.update(GitLabStateUpdates.addProject(project)).unsafeRunSync()

    def setupProject(project: Project, commits: CommitId*): Unit =
      self.update(GitLabStateUpdates.setupProject(project, webhookUri, commits: _*)).unsafeRunSync()

    def replaceRenkuProject(id: Id, project: RenkuProject): Unit =
      self.update(GitLabStateUpdates.replaceRenkuProject(id, project)).unsafeRunSync()

    def replaceCommits(id: Id, commits: CommitId*): Unit =
      self.update(GitLabStateUpdates.replaceCommits(id, commits)).unsafeRunSync()

    def removeProject(id: Id): Unit =
      self.update(GitLabStateUpdates.removeProject(id)).unsafeRunSync()
  }
}
