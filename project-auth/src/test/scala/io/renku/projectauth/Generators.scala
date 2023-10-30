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

import cats.syntax.all._
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.{Role, Slug, Visibility}
import io.renku.graph.model.{RenkuTinyTypeGenerators, persons}
import io.renku.generators.Generators.Implicits._
import org.scalacheck.Gen

object Generators {
  private val fixedIds = 1 to 800
  private val gitLabIds: Gen[persons.GitLabId] =
    Gen.oneOf(fixedIds).map(persons.GitLabId.apply)

  val memberGen: Gen[ProjectMember] = for {
    role <- RenkuTinyTypeGenerators.roleGen
    id   <- gitLabIds
  } yield ProjectMember(id, role)

  def projectAuthData: ProjectAuthDataBuilder = ProjectAuthDataBuilder()

  val projectAuthDataGen: Gen[ProjectAuthData] =
    projectAuthData.build

  final case class ProjectAuthDataBuilder(
      slug:       Gen[Slug] = RenkuTinyTypeGenerators.projectSlugs,
      members:    Gen[Set[ProjectMember]] = Gen.choose(1, 150).flatMap(n => Gen.listOfN(n, memberGen)).map(_.toSet),
      visibility: Gen[Visibility] = RenkuTinyTypeGenerators.projectVisibilities
  ) {
    def withMembers(members: (Int, Role)*): ProjectAuthDataBuilder =
      copy(members = Gen.const(members.map(t => ProjectMember(GitLabId(t._1), t._2)).toSet))

    def withRoles(roles: Role*): ProjectAuthDataBuilder =
      copy(members = roles.traverse(r => gitLabIds.map(id => ProjectMember(id, r))).map(_.toSet))

    def withSlug(slug: Slug): ProjectAuthDataBuilder =
      copy(slug = Gen.const(slug))

    def withVisibility(v: Visibility): ProjectAuthDataBuilder =
      copy(visibility = Gen.const(v))

    val build = for {
      s <- slug
      m <- members
      v <- visibility
    } yield ProjectAuthData(s, m, v)

    val stream = build.asStream
  }
}
