package io.renku.graph.acceptancetests.stubs.gitlab

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.{CommitData, State, Webhook}
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.Id
import io.renku.http.client.AccessToken
import org.http4s.Uri
import GitLabStateGenerators._
import cats.Monoid
import io.renku.graph.model.testentities.{Person, RenkuProject}

/** Collection of functions to update the state in [[GitLabApiStub]]. */
trait GitLabStateUpdates {
  type StateUpdate       = State => State
  type StateUpdateGet[A] = State => (State, A)

  final implicit class StateUpdateOps(self: StateUpdate) {
    def >>(next: StateUpdate): StateUpdate =
      self.andThen(next)
  }
  implicit def stateUpdateMonoid: Monoid[StateUpdate] =
    Monoid.instance(identity, _ >> _)

  def clearState: StateUpdate =
    _ => State.empty

  def addUser(id: GitLabId, token: AccessToken): StateUpdate =
    state => state.copy(users = state.users.updated(id, token))

  def addWebhook(projectId: Id, url: Uri): StateUpdate = state => {
    val nextId = state.webhooks.size
    state.copy(webhooks =
      Webhook(nextId, projectId, url) :: state.webhooks.filter(p => p.projectId != projectId || p.url != url)
    )
  }

  def removeWebhook(projectId: Id, hookId: Int): StateUpdateGet[Boolean] = state => {
    val filtered = state.webhooks.filterNot(h => h.projectId == projectId && h.webhookId == hookId)
    (state.copy(webhooks = filtered), filtered != state.webhooks)
  }

  def recordCommitData(projectId: Id, commits: Seq[CommitData]): StateUpdate = state => {
    val next = state.commits.updatedWith(projectId) {
      case Some(existing) => existing.concat(commits.toList).some
      case None           => NonEmptyList.fromList(commits.toList)
    }
    state.copy(commits = next)
  }

  def recordCommits(projectId: Id, commits: Seq[CommitId]): StateUpdate =
    recordCommitData(projectId, commits.map(id => commitData(id).generateOne))

  def addPerson(person: Person*): StateUpdate =
    addPersons(person.toSet)

  def addPersons(persons: Iterable[Person]): StateUpdate = state => {
    val ids = persons.flatMap(_.maybeGitLabId).toSet
    state.copy(persons = persons.toList ::: state.persons.filterNot(p => p.maybeGitLabId.exists(ids.contains)))
  }

  def addProject(project: Project): StateUpdate =
    state => state.copy(projects = project :: state.projects.filter(_.id != project.id))

  def addProject(project: Project, webhook: Uri): StateUpdate =
    addProject(project) >> addWebhook(project.id, webhook)

  def setupProject(project: Project, webhook: Uri, commits: CommitId*): StateUpdate =
    addProject(project, webhook) >>
      recordCommits(project.id, commits) >>
      addPersons(GitLabStateUpdates.findAllPersons(project.entitiesProject))
}

object GitLabStateUpdates extends GitLabStateUpdates {
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
