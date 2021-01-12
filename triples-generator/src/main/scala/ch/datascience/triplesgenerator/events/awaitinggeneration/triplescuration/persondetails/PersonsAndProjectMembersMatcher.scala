/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.awaitinggeneration.triplescuration.persondetails

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
