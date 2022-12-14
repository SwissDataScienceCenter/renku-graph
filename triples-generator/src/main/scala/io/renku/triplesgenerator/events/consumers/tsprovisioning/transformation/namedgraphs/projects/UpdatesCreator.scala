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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.projects

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas._
import io.renku.graph.model.entities._
import io.renku.graph.model.views.RdfResource
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes

private trait UpdatesCreator {
  def prepareUpdates(project:      Project, kgData: ProjectMutableData): List[SparqlQuery]
  def postUpdates(project:         Project): List[SparqlQuery]
  def dateCreatedDeletion(project: Project, kgData: ProjectMutableData): List[SparqlQuery]
}

private object UpdatesCreator extends UpdatesCreator {

  override def prepareUpdates(project: Project, kgData: ProjectMutableData): List[SparqlQuery] = List(
    nameDeletion(project, kgData),
    maybeParentDeletion(project, kgData),
    visibilityDeletion(project, kgData),
    descriptionDeletion(project, kgData),
    keywordsDeletion(project, kgData),
    agentDeletion(project, kgData),
    creatorDeletion(project, kgData)
  ).flatten

  private def nameDeletion(project: Project, kgData: ProjectMutableData) =
    Option.when(project.name != kgData.name) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project name delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { GRAPH $resource { $resource schema:name ?name } }
            |WHERE  { GRAPH $resource { $resource schema:name ?name } }
            |""".stripMargin
      )
    }

  private def maybeParentDeletion(project: Project, kgData: ProjectMutableData): Option[SparqlQuery] = {
    val maybeParent = project match {
      case p: Project with Parent => p.parentResourceId.some
      case _ => None
    }

    Option.when(
      kgData.maybeParentId match {
        case kgParent @ Some(_) if kgParent != maybeParent => true
        case _                                             => false
      }
    ) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project parent delete",
        Prefixes.of(prov -> "prov"),
        s"""|DELETE { GRAPH $resource { $resource prov:wasDerivedFrom ?maybeParent } }
            |WHERE  { GRAPH $resource { $resource prov:wasDerivedFrom ?maybeParent } }
            |""".stripMargin
      )
    }
  }

  private def visibilityDeletion(project: Project, kgData: ProjectMutableData) =
    Option.when(project.visibility != kgData.visibility) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project visibility delete",
        Prefixes.of(renku -> "renku"),
        s"""|DELETE { GRAPH $resource { $resource renku:projectVisibility ?visibility } }
            |WHERE  { GRAPH $resource { $resource renku:projectVisibility ?visibility } }
            |""".stripMargin
      )
    }

  private def descriptionDeletion(project: Project, kgData: ProjectMutableData) = Option.when(
    kgData.maybeDescription match {
      case kgDesc @ Some(_) if kgDesc != project.maybeDescription => true
      case _                                                      => false
    }
  ) {
    val resource = project.resourceId.showAs[RdfResource]
    SparqlQuery.of(
      name = "transformation - project description delete",
      Prefixes.of(schema -> "schema"),
      s"""|DELETE { GRAPH $resource { $resource schema:description ?description } }
          |WHERE  { GRAPH $resource { $resource schema:description ?description } }
          |""".stripMargin
    )
  }

  private def keywordsDeletion(project: Project, kgData: ProjectMutableData) =
    Option.when(kgData.keywords != project.keywords) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project keywords delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { GRAPH $resource { $resource schema:keywords ?keyword } }
            |WHERE  { GRAPH $resource { $resource schema:keywords ?keyword } }
            |""".stripMargin
      )
    }

  private def agentDeletion(project: Project, kgData: ProjectMutableData) = {
    val maybeAgent = project match {
      case _: NonRenkuProject => None
      case p: RenkuProject    => p.agent.some
    }
    Option.when(
      kgData.maybeAgent match {
        case kgAgent @ Some(_) if kgAgent != maybeAgent => true
        case _                                          => false
      }
    ) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project agent delete",
        Prefixes of schema -> "schema",
        s"""|DELETE { GRAPH $resource { $resource schema:agent ?agent } }
            |WHERE  { GRAPH $resource { $resource schema:agent ?agent } }
            |""".stripMargin
      )
    }
  }

  private def creatorDeletion(project: Project, kgData: ProjectMutableData) =
    Option.when(
      kgData.maybeCreatorId match {
        case kgCreator @ Some(_) if kgCreator != project.maybeCreator.map(_.resourceId) => true
        case _                                                                          => false
      }
    ) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project creator delete",
        Prefixes of schema -> "schema",
        s"""|DELETE { GRAPH $resource { $resource schema:creator ?creator } }
            |WHERE  { GRAPH $resource { $resource schema:creator ?creator } }
            |""".stripMargin
      )
    }

  override def dateCreatedDeletion(project: Project, kgData: ProjectMutableData) =
    Option
      .when(project.dateCreated != kgData.dateCreated) {
        val resource = project.resourceId.showAs[RdfResource]
        SparqlQuery.of(
          name = "transformation - project dateCreated delete",
          Prefixes.of(schema -> "schema"),
          s"""|DELETE { GRAPH $resource { $resource schema:dateCreated ?date } }
              |WHERE  { GRAPH $resource { $resource schema:dateCreated ?date } }
              |""".stripMargin
        )
      }
      .toList

  override def postUpdates(project: Project): List[SparqlQuery] =
    List(deduplicateDateCreated(project))

  /** Retain the lowest dateCreated date. */
  private def deduplicateDateCreated(project: Project) = {
    val resource = project.resourceId.showAs[RdfResource]
    SparqlQuery.of(
      name = "transformation - project date created deduplicate",
      Prefixes.of(schema -> "schema"),
      s"""|WITH $resource 
          |DELETE { ?p schema:dateCreated ?date }
          |WHERE {
          |  {
          |    SELECT (min(?cdate) as ?minDate)
          |    WHERE {
          |      ?_s a schema:Project;
          |          schema:dateCreated ?cdate.
          |    }
          |  }
          |  ?p a schema:Project;
          |     schema:dateCreated ?date.
          |  FILTER ( ?date != ?minDate )
          |}
          |""".stripMargin
    )
  }
}
