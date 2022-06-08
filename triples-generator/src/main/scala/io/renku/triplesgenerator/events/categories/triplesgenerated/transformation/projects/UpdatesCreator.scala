/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.projects

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas._
import io.renku.graph.model.entities._
import io.renku.graph.model.views.RdfResource
import io.renku.rdfstore.SparqlQuery
import io.renku.rdfstore.SparqlQuery.Prefixes

private trait UpdatesCreator {
  def prepareUpdates(project: Project, kgProjectInfo: ProjectMutableData): List[SparqlQuery]
}

private object UpdatesCreator extends UpdatesCreator {

  override def prepareUpdates(project: Project, kgProjectInfo: ProjectMutableData): List[SparqlQuery] = List(
    nameDeletion(project, kgProjectInfo),
    maybeParentDeletion(project, kgProjectInfo),
    visibilityDeletion(project, kgProjectInfo),
    descriptionDeletion(project, kgProjectInfo),
    keywordsDeletion(project, kgProjectInfo),
    agentDeletion(project, kgProjectInfo)
  ).flatten

  private def nameDeletion(project: Project, kgProjectInfo: ProjectMutableData) =
    Option.when(project.name != kgProjectInfo.name) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project name delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { $resource schema:name ?name }
            |WHERE  { $resource schema:name ?name }
            |""".stripMargin
      )
    }

  private def maybeParentDeletion(project: Project, kgProjectInfo: ProjectMutableData): Option[SparqlQuery] = {
    val maybeParent = project match {
      case p: Project with Parent => p.parentResourceId.some
      case _ => None
    }

    Option.when(
      kgProjectInfo.maybeParentId match {
        case Some(kgParent) if kgParent.some != maybeParent => true
        case _                                              => false
      }
    ) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project parent delete",
        Prefixes.of(prov -> "prov"),
        s"""|DELETE { $resource prov:wasDerivedFrom ?maybeParent }
            |WHERE  { $resource prov:wasDerivedFrom ?maybeParent }
            |""".stripMargin
      )
    }
  }

  private def visibilityDeletion(project: Project, kgProjectInfo: ProjectMutableData) =
    Option.when(project.visibility != kgProjectInfo.visibility) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project visibility delete",
        Prefixes.of(renku -> "renku"),
        s"""|DELETE { $resource renku:projectVisibility ?visibility }
            |WHERE  { $resource renku:projectVisibility ?visibility }
            |""".stripMargin
      )
    }

  private def descriptionDeletion(project: Project, kgProjectInfo: ProjectMutableData) = Option.when(
    kgProjectInfo.maybeDescription match {
      case Some(kgDescription) if kgDescription.some != project.maybeDescription => true
      case _                                                                     => false
    }
  ) {
    val resource = project.resourceId.showAs[RdfResource]
    SparqlQuery.of(
      name = "transformation - project description delete",
      Prefixes.of(schema -> "schema"),
      s"""|DELETE { $resource schema:description ?description }
          |WHERE  { $resource schema:description ?description }
          |""".stripMargin
    )
  }

  private def keywordsDeletion(project: Project, kgProjectInfo: ProjectMutableData) =
    Option.when(kgProjectInfo.keywords != project.keywords) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project keywords delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { $resource schema:keywords ?keyword }
            |WHERE  { $resource schema:keywords ?keyword }
            |""".stripMargin
      )
    }

  private def agentDeletion(project: Project, kgProjectInfo: ProjectMutableData) = {
    val maybeAgent = project match {
      case _: NonRenkuProject => None
      case p: RenkuProject    => p.agent.some
    }
    Option.when(
      kgProjectInfo.maybeAgent match {
        case Some(kgAgent) if kgAgent.some != maybeAgent => true
        case _                                           => false
      }
    ) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project description delete",
        Prefixes of schema -> "schema",
        s"""|DELETE { $resource schema:agent ?agent }
            |WHERE  { $resource schema:agent ?agent }
            |""".stripMargin
      )
    }
  }
}
