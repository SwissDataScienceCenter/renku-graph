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

package io.renku.projectauth

import io.renku.graph.model.{RenkuTinyTypeGenerators, persons}
import org.scalacheck.Gen

object Generators {
  private val fixedIds = 1 to 800
  private val gitLabIds: Gen[persons.GitLabId] =
    Gen.oneOf(fixedIds).map(persons.GitLabId.apply)

  val roleGen: Gen[Role] =
    Gen.oneOf(Role.all.toList)

  val memberGen: Gen[ProjectMember] = for {
    role <- roleGen
    id   <- gitLabIds
  } yield ProjectMember(id, role)

  val projectAuthDataGen: Gen[ProjectAuthData] = for {
    slug       <- RenkuTinyTypeGenerators.projectSlugs
    members    <- Gen.choose(0, 150).flatMap(n => Gen.listOfN(n, memberGen))
    visibility <- RenkuTinyTypeGenerators.projectVisibilities
  } yield ProjectAuthData(slug, members.toSet, visibility)
}
