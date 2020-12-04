package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.syntax.all._

private class PersonsAndProjectMembersMatcher {

  def merge(persons: Set[Person], projectMembers: Set[GitLabProjectMember]): Set[Person] =
    persons map { person =>
      projectMembers
        .find(byNameOrUsername(person))
        .map(addGitlabId(person))
        .getOrElse(person)
    }

  private def byNameOrUsername(person: Person)(member: GitLabProjectMember): Boolean =
    (member.name == person.name) || (member.username.value == person.name.value)

  private def addGitlabId(person: Person)(member: GitLabProjectMember): Person =
    person.copy(maybeGitLabId = member.id.some)
}
