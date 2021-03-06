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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import ch.datascience.graph.model.users.{Email, GitLabId, Name, ResourceId, Username}

private final case class GitLabProjectMember(id: GitLabId, username: Username, name: Name)

private final case class Person(id: ResourceId, maybeGitLabId: Option[GitLabId], name: Name, maybeEmail: Option[Email])
private object Person {
  def apply(id: ResourceId, name: Name, maybeEmail: Option[Email]): Person = Person(id, None, name, maybeEmail)
}

private final case class PersonRawData(id: ResourceId, names: List[Name], emails: List[Email])

private final case class CommitPerson(name: Name, email: Email)
