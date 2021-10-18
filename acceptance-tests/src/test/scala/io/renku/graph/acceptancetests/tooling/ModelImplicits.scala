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

package io.renku.graph.acceptancetests.tooling

import cats.data.NonEmptyList
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.testentities.Person
import io.renku.graph.model.users

trait ModelImplicits {

  implicit class PersonOps(person: Person) {

    def asMember(): (users.GitLabId, users.Username, users.Name) =
      (person.maybeGitLabId getOrElse userGitLabIds.generateOne, usernames.generateOne, person.name)

    def asMembersList(): NonEmptyList[(users.GitLabId, users.Username, users.Name)] =
      NonEmptyList.of(person.asMember())
  }
}
