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

package io.renku.projectauth.util

import cats.syntax.all._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{persons, projects}
import io.renku.projectauth.{ProjectAuth, ProjectMember}
import io.renku.triplesstore.client.sparql.{Fragment, VarName}
import io.renku.triplesstore.client.syntax._

final class SparqlSnippets(val projectId: VarName) {

  def changeVisibility(slug: projects.Slug, newValue: projects.Visibility): Fragment =
    sparql"""|DELETE { GRAPH ${ProjectAuth.graph} { ?id renku:visibility ?visibility } }
             |INSERT { GRAPH ${ProjectAuth.graph} { ?id renku:visibility ${newValue.asObject} } }
             |WHERE {
             |  GRAPH ${ProjectAuth.graph} {
             |    ?id a schema:Project;
             |        renku:slug ${slug.asObject};
             |        renku:visibility ?visibility.
             |  }
             |}""".stripMargin

  def memberProjects(userId: persons.GitLabId): Fragment =
    sparql"""
            |Graph renku:ProjectAuth {
            |   $projectId a schema:Project;
            |              renku:memberId ${userId.value}.
            |}
            | """.stripMargin

  def ownedProjects(userId: persons.GitLabId): Fragment =
    fr"""|GRAPH renku:ProjectAuth {
         |   $projectId a schema:Project;
         |              renku:memberRole ${ProjectMember(userId, projects.Role.Owner).encoded}.
         |}
         | """.stripMargin

  def visibleProjects(userId: Option[persons.GitLabId], selectedVisibility: Set[Visibility]): Fragment = {
    val visibilities =
      if (selectedVisibility.isEmpty) Visibility.all
      else selectedVisibility

    val parts = visibilities.map(visibilityFragment(userId)).filter(_.nonEmpty)
    val inner =
      if (parts.isEmpty) sparql"""VALUES ($projectId) {}"""
      else parts.toList.foldSmash(sparql"{ ", sparql" } UNION { ", sparql" }")
    sparql"""
            |Graph ${ProjectAuth.graph} {
            |  $projectId a schema:Project.
            |  $inner
            |}
            |""".stripMargin
  }

  private def visibilityFragment(userId: Option[persons.GitLabId])(v: Visibility) = v match {
    case Visibility.Public => publicProjects
    case Visibility.Internal =>
      if (userId.isDefined) internalProjects
      else Fragment.empty
    case Visibility.Private =>
      userId.map(privateProjects).getOrElse(Fragment.empty)
  }

  private def publicProjects: Fragment =
    sparql"$projectId renku:visibility ${Visibility.Public.value}."

  private def internalProjects: Fragment =
    sparql"$projectId renku:visibility ${Visibility.Internal.value}."

  private def privateProjects(user: persons.GitLabId): Fragment =
    sparql"""
            |$projectId renku:visibility ${Visibility.Private.value};
            |           renku:memberId ${user.value}.
            |""".stripMargin
}

object SparqlSnippets {
  def apply(projectVar: VarName): SparqlSnippets =
    new SparqlSnippets(projectVar)

  val default: SparqlSnippets =
    SparqlSnippets(VarName("projectId"))
}
