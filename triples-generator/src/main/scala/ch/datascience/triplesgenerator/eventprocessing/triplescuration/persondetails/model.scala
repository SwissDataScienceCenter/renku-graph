package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import ch.datascience.graph.model.users.{Email, GitLabId, Name, ResourceId, Username}

private final case class GitLabProjectMember(id: GitLabId, username: Username, name: Name)

private final case class Person(id: ResourceId, maybeGitLabId: Option[GitLabId], name: Name, maybeEmail: Option[Email])
private object Person {
  def apply(id: ResourceId, name: Name, maybeEmail: Option[Email]): Person = Person(id, None, name, maybeEmail)
}
