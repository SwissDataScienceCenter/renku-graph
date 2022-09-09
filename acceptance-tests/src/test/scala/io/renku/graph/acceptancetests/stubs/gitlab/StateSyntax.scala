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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.{CommitData, PushEvent, State, Webhook}
import io.renku.graph.model.GraphModelGenerators
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.{Id, Path, Visibility}
import io.renku.graph.model.testentities.{Person, RenkuProject}
import io.renku.http.client.AccessToken
import io.renku.http.server.security.model.AuthUser
import org.http4s.Uri
import org.scalacheck.Gen

import java.time.Instant

trait StateSyntax {
  def commitData(commitId: CommitId): Gen[CommitData] =
    for {
      authorName     <- GraphModelGenerators.personNames
      authorEmail    <- GraphModelGenerators.personEmails
      committerName  <- GraphModelGenerators.personNames
      committerEmail <- GraphModelGenerators.personEmails
      message        <- Generators.nonEmptyStrings()
    } yield CommitData(
      commitId,
      Person(authorName, authorEmail),
      Person(committerName, committerEmail),
      Instant.now(),
      message,
      Nil
    )

  final implicit class StateOps(self: State) {

    def addAuthenticated(id: GitLabId, token: AccessToken): State =
      self.copy(users = self.users.updated(id, token))

    def addAuthenticated(user: AuthUser): State =
      addAuthenticated(user.id, user.accessToken)

    def addProject(project: Project): State =
      self.copy(projects = project :: self.projects.filter(_.id != project.id))

    def addProject(project: Project, webhook: Uri): State =
      addProject(project).addWebhook(project.id, webhook)

    def setupProject(project: Project, webhook: Uri, commits: CommitId*): State =
      addProject(project, webhook)
        .recordCommits(project.id, commits)
        .addPersons(StateSyntax.findAllPersons(project.entitiesProject))

    def addPerson(person: Person*): State =
      addPersons(person.toSet)

    def addPersons(persons: Iterable[Person]): State = {
      val ids = persons.flatMap(_.maybeGitLabId).toSet
      self.copy(persons = persons.toList ::: self.persons.filterNot(p => p.maybeGitLabId.exists(ids.contains)))
    }

    def addWebhook(projectId: Id, url: Uri): State = {
      val nextId = self.webhooks.size
      self.copy(webhooks =
        Webhook(nextId, projectId, url) :: self.webhooks.filter(p => p.projectId != projectId || p.url != url)
      )
    }

    def removeWebhook(projectId: Id, hookId: Int): (State, Boolean) = {
      val filtered = self.webhooks.filterNot(h => h.projectId == projectId && h.webhookId == hookId)
      (self.copy(webhooks = filtered), filtered != self.webhooks)
    }

    def recordCommitData(projectId: Id, commits: Seq[CommitData]): State = {
      val next = self.commits.updatedWith(projectId) {
        case Some(existing) => existing.concat(commits.toList).some
        case None           => NonEmptyList.fromList(commits.toList)
      }
      self.copy(commits = next)
    }

    def recordCommits(projectId: Id, commits: Seq[CommitId]): State =
      recordCommitData(projectId, commits.map(id => commitData(id).generateOne))

    def commitsFor(projectId: Id, user: Option[GitLabId]): List[CommitData] =
      findProject(projectId, user).toList
        .flatMap(_ => self.commits.get(projectId).map(_.toList))
        .flatten

    def findCommit(projectId: Id, user: Option[GitLabId], sha: CommitId): Option[CommitData] =
      commitsFor(projectId, user).find(_.commitId == sha)

    def findPushEvents(projectId: Id, user: Option[GitLabId]): List[PushEvent] =
      commitsFor(projectId, user).map(_.toPushEvent(projectId))

    def findUserByToken(token: AccessToken): Option[AuthUser] =
      self.users.find(_._2 == token).map(AuthUser.tupled)

    def findPersonById(id: GitLabId): Option[Person] =
      self.persons.find(_.maybeGitLabId == id.some)

    def projectsFor(user: Option[GitLabId]): List[Project] =
      self.projects.filter { p =>
        p.entitiesProject.visibility == Visibility.Public ||
        user.exists(p.entitiesProject.members.flatMap(_.maybeGitLabId).contains) ||
        p.entitiesProject.maybeCreator.flatMap(_.maybeGitLabId) == user
      }

    def findProject(id: Id, user: Option[GitLabId]): Option[Project] =
      projectsFor(user).find(p => p.id == id)

    def findProject(path: Path, user: Option[GitLabId]): Option[Project] =
      projectsFor(user).find(p => p.path == path)

    def findWebhooks(projectId: Id): List[Webhook] =
      self.webhooks.filter(_.projectId == projectId)
  }

  final implicit class CommitDataOps(self: CommitData) {
    def toPushEvent(projectId: Id): PushEvent =
      PushEvent(projectId, self.commitId, GraphModelGenerators.personGitLabIds.generateOne, self.author.name)
  }
}

object StateSyntax extends StateSyntax {
  // TODO: this is a copy from ProjectFunctions which is not in scope
  private lazy val findAllPersons: RenkuProject => Set[Person] = project =>
    project.members ++
      project.maybeCreator ++
      project.activities.map(_.author) ++
      project.datasets.flatMap(_.provenance.creators.toList.toSet) ++
      project.activities.flatMap(_.association.agent match {
        case p: Person => Option(p)
        case _ => Option.empty[Person]
      })
}
