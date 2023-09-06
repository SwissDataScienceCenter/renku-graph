package io.renku.projectauth.util

import cats.syntax.all._
import io.renku.graph.model.persons
import io.renku.graph.model.projects.Visibility
import io.renku.projectauth.ProjectAuth
import io.renku.triplesstore.client.sparql.{Fragment, VarName}
import io.renku.triplesstore.client.syntax._

object SparqlSnippets {
  val projectId         = VarName("projectId")
  val projectVisibility = VarName("projectVisibility")

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
