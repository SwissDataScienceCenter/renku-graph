/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.renku.graph.model.persons
import io.renku.graph.model.testentities.Person

trait ModelImplicits {

  implicit class PersonOps(person: Person) {

    def asMember(): (persons.GitLabId, persons.Username, persons.Name) =
      (person.maybeGitLabId getOrElse personGitLabIds.generateOne, usernames.generateOne, person.name)

    def asMembersList(): NonEmptyList[(persons.GitLabId, persons.Username, persons.Name)] =
      NonEmptyList.of(person.asMember())
  }
}
